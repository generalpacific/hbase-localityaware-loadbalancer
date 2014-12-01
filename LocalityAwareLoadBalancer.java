/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.balancer;

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.rest.model.StorageClusterStatusModel;

import java.io.IOException;
import java.util.*;

/**
 * Makes decisions about the placement and movement of Regions across
 * RegionServers.
 *
 * <p>Cluster-wide load balancing will occur only when there are no regions in
 * transition and according to a fixed period of a time using {@link #balanceCluster(java.util.Map)}.
 *
 * <p>Inline region placement with {@link #immediateAssignment} can be used when
 * the Master needs to handle closed regions that it currently does not have
 * a destination set for.  This can happen during master failover.
 *
 * <p>On cluster startup, bulk assignment can be used to determine
 * locations for all Regions in a cluster.
 *
 * <p>This classes produces plans for the {@link org.apache.hadoop.hbase.master.AssignmentManager} to execute.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class LocalityAwareLoadBalancer extends BaseLoadBalancer {
  private static final Log LOG = LogFactory.getLog(LocalityAwareLoadBalancer.class);


  private static Set<HRegionInfo> previouslyMovedRegions = new HashSet<HRegionInfo>();

  private static final int TABLE_BALANCER_WEIGHT = 2;
  private static final int SERVER_BALANCER_WEIGHT = 3;
  private static final int NUMBER_OF_REGIONS_TO_MOVE = 1;
  private static final int LOCALITY_WEIGHT = 4;
  private static final int STICKINESS_WEIGHT = 2;

  private RegionLocationFinder regionLocationFinder = new RegionLocationFinder();

  @Override
  public Configuration getConf() {
    return super.getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    regionLocationFinder.setConf(conf);
  }

  /**
   * This implements the Locality Aware Load Balancer.
   * Information for the algorithm can be found here: https://issues.apache.org/jira/browse/HBASE-10075
   *
   * @param clusterMap Map of regionservers and their load/region information to
   *                   a list of their most loaded regions
   * @return a list of regions to be moved, including source and destination,
   *         or null if cluster is already balanced
   */
  public List<RegionPlan> balanceCluster(
      Map<ServerName, List<HRegionInfo>> clusterMap) {
    long startTime = System.currentTimeMillis();

    ClusterLoadState cs = new ClusterLoadState(clusterMap);

    if (!this.needsBalance(cs)) {
      LOG.info("No balancing needed. Returning null.");
      return null;
    }

    Cluster cluster = new Cluster(clusterMap, new HashMap<String, Deque<RegionLoad>>(), regionLocationFinder);
    NavigableMap<ServerAndLoad, List<HRegionInfo>> serversByLoad = cs.getServersByLoad();
    int numRegions = cs.getNumRegions();

    PriorityQueue<RegionServerRegionAffinity> queue = new PriorityQueue<RegionServerRegionAffinity>();


    // calculate allTableRegionNumber = total number of regions per table.
    Map<Integer, Integer> allTableRegionNumberMap = new HashMap<Integer , Integer>();
    for (int i = 0; i < cluster.numServers; ++i) {
      for (int j = 0; j < cluster.numTables; ++j) {
        if (allTableRegionNumberMap.containsKey(j)) {
          Integer integer = allTableRegionNumberMap.get(j);
          integer = integer + cluster.numRegionsPerServerPerTable[i][j];
          allTableRegionNumberMap.put(j, integer);
        } else {
          allTableRegionNumberMap.put(j, cluster.numRegionsPerServerPerTable[i][j]);
        }
      }
    }

    for (ServerAndLoad server : serversByLoad.keySet()) {
      List<HRegionInfo> hRegionInfos = serversByLoad.get(server);
      double regionAffinityNumber = (1 - hRegionInfos.size() / numRegions) * SERVER_BALANCER_WEIGHT;
      double tableRegionAffinityNumber = 0;
      // Calculate allTableRegionNumber
      for (HRegionInfo hRegionInfo : hRegionInfos) {
        TableName table = hRegionInfo.getTable();
        String tableName = table.getNameAsString();
        int tableIndex = cluster.tablesToIndex.get(tableName);
        int serverIndex = cluster.serversToIndex.get(server.getServerName().getHostAndPort());
        tableRegionAffinityNumber = (1 -
                cluster.numRegionsPerServerPerTable[serverIndex][tableIndex] / allTableRegionNumberMap.get(tableIndex)) * TABLE_BALANCER_WEIGHT;
        double finalAffinity = regionAffinityNumber +
                tableRegionAffinityNumber +
                getLocalityIndex(hRegionInfo, server) * LOCALITY_WEIGHT +
                getStickinessWeight(hRegionInfo);
        queue.add(new RegionServerRegionAffinity(server, hRegionInfo, finalAffinity));
        LOG.info("Affinity between server=" + server.getServerName() + " and region="+ hRegionInfo.getRegionNameAsString() + " is " + finalAffinity);
      }
    }

    LOG.info("All server and region affinities: " + queue);

    List<RegionPlan> regionsToReturn = new ArrayList<RegionPlan>();

    if (queue.isEmpty()) {
      long endTime = System.currentTimeMillis();
      LOG.info("Calculated a load balance in " + (endTime-startTime) + "ms. " +
              "No regions to move");
      return regionsToReturn;
    }

    // Get top NUMBER_OF_REGIONS_TO_MOVE
    List<RegionServerRegionAffinity> listOfRegionsToMove = new ArrayList<RegionServerRegionAffinity>();
    for (int i = 0; i < NUMBER_OF_REGIONS_TO_MOVE; ++i) {
      if (queue.isEmpty()) {
        continue;
      }
      listOfRegionsToMove.add(queue.peek());
    }

    // Search for the most affine servers to these Region Servers
    for (RegionServerRegionAffinity regionServerRegionAffinity : listOfRegionsToMove) {
      HRegionInfo hRegionInfoToMove = regionServerRegionAffinity.getHRegionInfo();
      ServerAndLoad serverToMove = null;
      double maxAffinity = Double.MIN_VALUE;
      for (ServerAndLoad server : serversByLoad.keySet()) {
        if (server.equals(regionServerRegionAffinity.getServer())) {
          continue;
        }
        List<HRegionInfo> hRegionInfos = serversByLoad.get(server);
        double regionAffinityNumber = (1 - hRegionInfos.size() / numRegions) * SERVER_BALANCER_WEIGHT;
        TableName table = hRegionInfoToMove.getTable();
        String tableNameAsString = table.getNameAsString();
        int serverIndex = cluster.serversToIndex.get(server.getServerName().getHostAndPort());
        int tableRegionAffinityNumber = 0;
        if (cluster.tablesToIndex.containsKey(tableNameAsString)) {
          Integer tableIndex = cluster.tablesToIndex.get(tableNameAsString);
          tableRegionAffinityNumber = (1 -
                  cluster.numRegionsPerServerPerTable[serverIndex][tableIndex] / allTableRegionNumberMap.get(tableIndex)) *
                  TABLE_BALANCER_WEIGHT;
        } else {
          LOG.error("Table " + tableNameAsString + "not present in cluster.tablesToIndex");
        }
        double finalAffinity = regionAffinityNumber +
                tableRegionAffinityNumber +
                getLocalityIndex(hRegionInfoToMove, server) * LOCALITY_WEIGHT +
                getStickinessWeight(hRegionInfoToMove);
        if (finalAffinity > maxAffinity) {
          maxAffinity = finalAffinity;
          serverToMove = server;
        }
        regionsToReturn.add(new RegionPlan(hRegionInfoToMove, regionServerRegionAffinity.getServer().getServerName(), serverToMove.getServerName()));
      }

    }

    LOG.info("Returning plan: " + regionsToReturn);

    // Reset previuosly moved regions and add new regions
    previouslyMovedRegions.clear();
    for (RegionPlan regionPlan : regionsToReturn) {
      previouslyMovedRegions.add(regionPlan.getRegionInfo());
    }

    long endTime = System.currentTimeMillis();
    LOG.info("Calculated a load balance in " + (endTime-startTime) + "ms. " +
            "Moving " + regionsToReturn.size() + " regions");
    return regionsToReturn;
  }

  /**
   * Class to store and compare regionserver - region affinities.
   */
  class RegionServerRegionAffinity implements  Comparable<RegionServerRegionAffinity>{
    private ServerAndLoad server;
    private HRegionInfo hRegionInfo;
    private double affinity;

    public ServerAndLoad getServer() {
      return server;
    }

    public HRegionInfo getHRegionInfo() {
      return hRegionInfo;
    }

    public double getAffinity() {
      return affinity;
    }

    public RegionServerRegionAffinity(ServerAndLoad server, HRegionInfo hRegionInfo, double affinity) {
      this.server = server;
      this.hRegionInfo = hRegionInfo;
      this.affinity = affinity;
    }

    @Override
    public int compareTo(RegionServerRegionAffinity regionServerRegionAffinity) {
      if (regionServerRegionAffinity.affinity > affinity) {
        return -1;
      }
      return 1;
    }

    @Override
    public String toString() {
      return server.getServerName() + " # " + hRegionInfo.getRegionNameAsString() + " # " + affinity;
    }
  }

  private float getLocalityIndex(HRegionInfo region, ServerAndLoad server) {
    try {
      //HTableDescriptor tableDescriptor = getTableDescriptor(region.getTable());
      HTableDescriptor tableDescriptor = this.services.getTableDescriptors().get(region.getTable());
      if (tableDescriptor != null) {
        HDFSBlocksDistribution blocksDistribution =
                HRegion.computeHDFSBlocksDistribution(getConf(), tableDescriptor, region);
        return blocksDistribution.getBlockLocalityIndex(server.toString());

      }
    } catch (IOException ioe) {
      LOG.debug("IOException during HDFSBlocksDistribution computation. for " + "region = "
              + region.getEncodedName(), ioe);
    }
    return Float.MAX_VALUE;
  }

  private double getStickinessWeight(HRegionInfo hRegionInfo) {
    int value = previouslyMovedRegions.contains(hRegionInfo) ? 1 : 0;
    return value * STICKINESS_WEIGHT;
  }
}
