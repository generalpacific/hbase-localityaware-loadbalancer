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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

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

    float average = cs.getLoadAverage(); // for logging
    int ceiling = (int) Math.ceil(average * (1 + slop));
    NavigableMap<ServerAndLoad, List<HRegionInfo>> serversByLoad = cs.getServersByLoad();

    if (!this.needsBalance(cs)) {
      /*LOG.info("Skipping load balancing because balanced cluster; " +
              "servers=" + cs.getNumServers() + " " +
              "regions=" + cs.getNumRegions() + " average=" + average + " " +
              "mostloaded=" + serversByLoad.lastKey().getLoad() +
              " leastloaded=" + serversByLoad.firstKey().getLoad());*/
      return null;
    }

    Cluster cluster = new Cluster(clusterMap, new HashMap<String, Deque<RegionLoad>>(), regionLocationFinder);
    int numRegions = cs.getNumRegions();

    LOG.info(" ####################################################################################");
    LOG.info(" Before Locality-aware Balancing");
    LOG.info(" Average=" + average + " Ce=" + ceiling);
    for (ServerAndLoad server : serversByLoad.keySet()) {
      LOG.info("---------------" + "Server Name: " + server.getServerName() + "---------------");
      List<HRegionInfo> hRegionInfos = serversByLoad.get(server);
      LOG.info("Number of Regions:" + hRegionInfos.size());
      for (HRegionInfo hRegionInfo : hRegionInfos){
        LOG.info(String.format("Name of Region: %s ", hRegionInfo.getRegionNameAsString()));
        //LOG.info(String.format("Size of Region in number of rows"+(Bytes.toInt(hRegionInfo.getStartKey())-Bytes.toInt(hRegionInfo.getEndKey()))));
        LOG.info("Start Key: " + Bytes.toString(hRegionInfo.getStartKey()));
        LOG.info("End Key: " + Bytes.toString(hRegionInfo.getEndKey()));
      }
      LOG.info("------------------------------------------------------------------------------");
    }

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

    List<RegionPlan> regionsToReturn = new ArrayList<RegionPlan>();

    for (ServerAndLoad server : serversByLoad.keySet()) {
      List<HRegionInfo> hRegionInfos = serversByLoad.get(server);
      // Check if number of regions on current server is greater than floor.
      // Continue only if number regions is greater than floor.
      if (hRegionInfos.size() <= ceiling) {
        LOG.debug("Number of HRegions <= ceiling (" + hRegionInfos.size() + " <= " + ceiling + ")");
        continue;
      }
      PriorityQueue<RegionServerRegionAffinity> queue = new PriorityQueue<RegionServerRegionAffinity>();
      int numberOfRegionsToMove = hRegionInfos.size() - ceiling;
      double regionAffinityNumber = (1 - hRegionInfos.size() / numRegions) * SERVER_BALANCER_WEIGHT;
      double tableRegionAffinityNumber = 0;
      // Calculate allTableRegionNumber
      for (HRegionInfo hRegionInfo : hRegionInfos) {
        // Do not move metaregion.
        if (hRegionInfo.isMetaRegion()) {
          continue;
        }
        TableName table = hRegionInfo.getTable();
        String tableName = table.getNameAsString();
        int tableIndex = cluster.tablesToIndex.get(tableName);
        int serverIndex = cluster.serversToIndex.get(server.getServerName().getHostAndPort());
        tableRegionAffinityNumber = (1 -
                cluster.numRegionsPerServerPerTable[serverIndex][tableIndex] / allTableRegionNumberMap.get(tableIndex)) * TABLE_BALANCER_WEIGHT;
        float localityIndex = getLocalityIndex(hRegionInfo, server) * LOCALITY_WEIGHT;
        LOG.info("tableRegionaffinity: " + tableRegionAffinityNumber);
        LOG.info("regionAffinityNUmber: " + regionAffinityNumber);
        LOG.info("localityIndex: " + localityIndex);
        double finalAffinity = regionAffinityNumber +
                tableRegionAffinityNumber +
                localityIndex +
                getStickinessWeight(hRegionInfo);
        queue.add(new RegionServerRegionAffinity(server, hRegionInfo, finalAffinity));
        LOG.info("Affinity between server=" + server.getServerName() + " and region="+ hRegionInfo.getRegionNameAsString() + " is " + finalAffinity);
      }

      LOG.info("Number of regions to move=" + numberOfRegionsToMove + " All server and region affinities: " + queue);

      // Get top numberOfRegionsToMove
      List<RegionServerRegionAffinity> listOfRegionsToMove = new ArrayList<RegionServerRegionAffinity>();
      for (int i = 0; i < numberOfRegionsToMove; ++i) {
        if (queue.isEmpty()) {
          continue;
        }
        listOfRegionsToMove.add(queue.poll());
      }

      // Search for the most affine servers to these listOfRegionsToMove
      for (RegionServerRegionAffinity regionServerRegionAffinity : listOfRegionsToMove) {
        HRegionInfo hRegionInfoToMove = regionServerRegionAffinity.getHRegionInfo();
        ServerAndLoad serverToMove = null;
        double maxAffinity = Double.MIN_VALUE;
        // Get the most affine server to hRegionInfoToMove
        for (ServerAndLoad activeServer : serversByLoad.keySet()) {
          hRegionInfos = serversByLoad.get(activeServer);
          if (activeServer.equals(regionServerRegionAffinity.getServer())) {
            continue;
          }
          if (hRegionInfos.size() >= ceiling) {
            LOG.debug("Number of HRegions >= ceiling (" + hRegionInfos.size() + " >= " + ceiling + ")");
            continue;
          }
          regionAffinityNumber = (1 - hRegionInfos.size() / numRegions) * SERVER_BALANCER_WEIGHT;
          TableName table = hRegionInfoToMove.getTable();
          String tableNameAsString = table.getNameAsString();
          int serverIndex = cluster.serversToIndex.get(activeServer.getServerName().getHostAndPort());
          tableRegionAffinityNumber = 0;
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
                  getLocalityIndex(hRegionInfoToMove, activeServer) * LOCALITY_WEIGHT +
                  getStickinessWeight(hRegionInfoToMove);
          if (finalAffinity > maxAffinity) {
            maxAffinity = finalAffinity;
            serverToMove = activeServer;
          }
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
      HTableDescriptor tableDescriptor = this.services.getTableDescriptors().get(region.getTable());
      if (tableDescriptor != null) {
        HDFSBlocksDistribution blocksDistribution =
                HRegion.computeHDFSBlocksDistribution(getConf(), tableDescriptor, region);
        return blocksDistribution.getBlockLocalityIndex(server.getServerName().getHostname());

      }
    } catch (IOException ioe) {
      LOG.debug("IOException during HDFSBlocksDistribution computation. for " + "region = "
              + region.getEncodedName(), ioe);
    }
    return Float.MAX_VALUE;
  }

  private double getStickinessWeight(HRegionInfo hRegionInfo) {
    int value = previouslyMovedRegions.contains(hRegionInfo) ? 0 : 1;
    return value * STICKINESS_WEIGHT;
  }
}
