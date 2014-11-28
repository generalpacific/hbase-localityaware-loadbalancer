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
  private static final Random RANDOM = new Random(System.currentTimeMillis());

  private static final int TABLE_BALANCER_WEIGHT = 2;
  private static final int SERVER_BALANCER_WEIGHT = 3;
  private static final int NUMBER_OF_REGIONS_TO_MOVE = 1;
  private static final int LOCALITY_WEIGHT = 4;
  private static final int STICKINESS_WEIGHT = 2;

  private RegionInfoComparator riComparator = new RegionInfoComparator();
  private RegionPlan.RegionPlanComparator rpComparator = new RegionPlan.RegionPlanComparator();
  private RegionLocationFinder regionLocationFinder = new RegionLocationFinder();


  /**
   * Stores additional per-server information about the regions added/removed
   * during the run of the balancing algorithm.
   *
   * For servers that shed regions, we need to track which regions we have already
   * shed. <b>nextRegionForUnload</b> contains the index in the list of regions on
   * the server that is the next to be shed.
   */
  static class BalanceInfo {

    private final int nextRegionForUnload;
    private int numRegionsAdded;

    public BalanceInfo(int nextRegionForUnload, int numRegionsAdded) {
      this.nextRegionForUnload = nextRegionForUnload;
      this.numRegionsAdded = numRegionsAdded;
    }

    int getNextRegionForUnload() {
      return nextRegionForUnload;
    }

    int getNumRegionsAdded() {
      return numRegionsAdded;
    }

    void setNumRegionsAdded(int numAdded) {
      this.numRegionsAdded = numAdded;
    }
  }

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
   * Generate a global load balancing plan according to the specified map of
   * server information to the most loaded regions of each server.
   *
   * The load balancing invariant is that all servers are within 1 region of the
   * average number of regions per server.  If the average is an integer number,
   * all servers will be balanced to the average.  Otherwise, all servers will
   * have either floor(average) or ceiling(average) regions.
   *
   * HBASE-3609 Modeled regionsToMove using Guava's MinMaxPriorityQueue so that
   *   we can fetch from both ends of the queue. 
   * At the beginning, we check whether there was empty region server 
   *   just discovered by Master. If so, we alternately choose new / old
   *   regions from head / tail of regionsToMove, respectively. This alternation
   *   avoids clustering young regions on the newly discovered region server.
   *   Otherwise, we choose new regions from head of regionsToMove.
   *   
   * Another improvement from HBASE-3609 is that we assign regions from
   *   regionsToMove to underloaded servers in round-robin fashion.
   *   Previously one underloaded server would be filled before we move onto
   *   the next underloaded server, leading to clustering of young regions.
   *   
   * Finally, we randomly shuffle underloaded servers so that they receive
   *   offloaded regions relatively evenly across calls to balanceCluster().
   *         
   * The algorithm is currently implemented as such:
   *
   * <ol>
   * <li>Determine the two valid numbers of regions each server should have,
   *     <b>MIN</b>=floor(average) and <b>MAX</b>=ceiling(average).
   *
   * <li>Iterate down the most loaded servers, shedding regions from each so
   *     each server hosts exactly <b>MAX</b> regions.  Stop once you reach a
   *     server that already has &lt;= <b>MAX</b> regions.
   *     <p>
   *     Order the regions to move from most recent to least.
   *
   * <li>Iterate down the least loaded servers, assigning regions so each server
   *     has exactly </b>MIN</b> regions.  Stop once you reach a server that
   *     already has &gt;= <b>MIN</b> regions.
   *
   *     Regions being assigned to underloaded servers are those that were shed
   *     in the previous step.  It is possible that there were not enough
   *     regions shed to fill each underloaded server to <b>MIN</b>.  If so we
   *     end up with a number of regions required to do so, <b>neededRegions</b>.
   *
   *     It is also possible that we were able to fill each underloaded but ended
   *     up with regions that were unassigned from overloaded servers but that
   *     still do not have assignment.
   *
   *     If neither of these conditions hold (no regions needed to fill the
   *     underloaded servers, no regions leftover from overloaded servers),
   *     we are done and return.  Otherwise we handle these cases below.
   *
   * <li>If <b>neededRegions</b> is non-zero (still have underloaded servers),
   *     we iterate the most loaded servers again, shedding a single server from
   *     each (this brings them from having <b>MAX</b> regions to having
   *     <b>MIN</b> regions).
   *
   * <li>We now definitely have more regions that need assignment, either from
   *     the previous step or from the original shedding from overloaded servers.
   *     Iterate the least loaded servers filling each to <b>MIN</b>.
   *
   * <li>If we still have more regions that need assignment, again iterate the
   *     least loaded servers, this time giving each one (filling them to
   *     </b>MAX</b>) until we run out.
   *
   * <li>All servers will now either host <b>MIN</b> or <b>MAX</b> regions.
   *
   *     In addition, any server hosting &gt;= <b>MAX</b> regions is guaranteed
   *     to end up with <b>MAX</b> regions at the end of the balancing.  This
   *     ensures the minimal number of regions possible are moved.
   * </ol>
   *
   * TODO: We can at-most reassign the number of regions away from a particular
   *       server to be how many they report as most loaded.
   *       Should we just keep all assignment in memory?  Any objections?
   *       Does this mean we need HeapSize on HMaster?  Or just careful monitor?
   *       (current thinking is we will hold all assignments in memory)
   *
   * @param clusterMap Map of regionservers and their load/region information to
   *                   a list of their most loaded regions
   * @return a list of regions to be moved, including source and destination,
   *         or null if cluster is already balanced
   */
  public List<RegionPlan> balanceCluster(
      Map<ServerName, List<HRegionInfo>> clusterMap) {
    LOG.info("PRASHANT ###########################################");
    boolean emptyRegionServerPresent = false;
    long startTime = System.currentTimeMillis();

    ClusterLoadState cs = new ClusterLoadState(clusterMap);

    if (!this.needsBalance(cs)) return null;

    Cluster cluster = new Cluster(clusterMap, new HashMap<String, Deque<RegionLoad>>(), regionLocationFinder);
    int numServers = cs.getNumServers();
    NavigableMap<ServerAndLoad, List<HRegionInfo>> serversByLoad = cs.getServersByLoad();
    int numRegions = cs.getNumRegions();
    int min = numRegions / numServers;
    int max = numRegions % numServers == 0 ? min : min + 1;

    PriorityQueue<RegionServerRegionAffinity> queue = new PriorityQueue<RegionServerRegionAffinity>();

    for (ServerAndLoad server : serversByLoad.keySet()) {

      List<HRegionInfo> hRegionInfos = serversByLoad.get(server);
      double regionAffinityNumber = (1 - hRegionInfos.size() / numRegions) * SERVER_BALANCER_WEIGHT;
      // TODO Instead of map use numRegionsPerServerPerTable to get tableRegionAffinityNumber
      double tableRegionAffinityNumber = 0;
      // calculate tableRegionNumberOnServer
      Map<TableName, Integer> tableRegionCountMap = new HashMap<TableName, Integer>();
      for (HRegionInfo hRegionInfo : hRegionInfos) {
          TableName table = hRegionInfo.getTable();
          if (tableRegionCountMap.containsKey(table)) {
              Integer integer = tableRegionCountMap.get(table);
              integer++;
              tableRegionCountMap.put(table, integer);
          } else {
            tableRegionCountMap.put(table, 1);
          }
      }
      // Calculate allTableRegionNumber
      for (HRegionInfo hRegionInfo : hRegionInfos) {
          TableName table = hRegionInfo.getTable();
          // TODO What should the value of i be is you dont have the table
          int i = 1;
          String tableName = table.getNameAsString();
          LOG.info("Trying for table: " + tableName);
          if (cluster.tablesToIndex.containsKey(tableName)) {
            LOG.info("Got tableToIndex: " + cluster.tablesToIndex.get(tableName));
            LOG.info("cluster.numMaxRegionsPerTable : " + cluster.numMaxRegionsPerTable);
            i = cluster.numMaxRegionsPerTable[cluster.tablesToIndex.get(tableName)];
          } else {
            LOG.error("Table " + tableName + "not present in cluster.tablesToIndex");
          }
          tableRegionAffinityNumber = (1 - tableRegionCountMap.get(table) / i) * TABLE_BALANCER_WEIGHT;
          // TODO add stickiness
          double finalAffinity = regionAffinityNumber + tableRegionAffinityNumber + getLocalityIndex(hRegionInfo, server) * LOCALITY_WEIGHT;
          queue.add(new RegionServerRegionAffinity(server, hRegionInfo, finalAffinity));
          LOG.info("Affinity between server=" + server.getServerName() + " and region="+ hRegionInfo.getRegionNameAsString() + " is " + finalAffinity);
      }
    }

    LOG.info("All server and region affinities: " + queue);


    // Get top NUMBER_OF_REGIONS_TO_MOVE
    List<RegionServerRegionAffinity> listOfRegionsToMove = new ArrayList<RegionServerRegionAffinity>();
    for (int i = 0; i < NUMBER_OF_REGIONS_TO_MOVE; ++i) {
      if (queue.isEmpty()) {
        continue;
      }
      listOfRegionsToMove.add(queue.peek());
    }

    List<RegionPlan> regionsToReturn = new ArrayList<RegionPlan>();

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
        int tableRegionNumberOnServer = 0;
        for (HRegionInfo hRegionInfo: hRegionInfos) {
          if (table.equals(hRegionInfo.getTable())) {
            tableRegionNumberOnServer++;
          }
        }
        String tableNameAsString = table.getNameAsString();
        int allTableRegionNumberOnServer = 1;
        if (cluster.tablesToIndex.containsKey(tableNameAsString)) {
          LOG.info("Got tableToIndex: " + cluster.tablesToIndex.get(tableNameAsString));
          LOG.info("cluster.numMaxRegionsPerTable : " + cluster.numMaxRegionsPerTable);
          allTableRegionNumberOnServer = cluster.numMaxRegionsPerTable[cluster.tablesToIndex.get(tableNameAsString)];
        } else {
          LOG.error("Table " + tableNameAsString + "not present in cluster.tablesToIndex");
        }

        int tableRegionAffinityNumber = (1 - tableRegionNumberOnServer / allTableRegionNumberOnServer) * TABLE_BALANCER_WEIGHT;
        double finalAffinity = regionAffinityNumber + tableRegionAffinityNumber + getLocalityIndex(hRegionInfoToMove, server);
        if (finalAffinity > maxAffinity) {
          maxAffinity = finalAffinity;
          serverToMove = server;
        }
        regionsToReturn.add(new RegionPlan(hRegionInfoToMove, regionServerRegionAffinity.getServer().getServerName(), serverToMove.getServerName()));
      }

    }

    LOG.info("Returning plan: " + regionsToReturn);
    return regionsToReturn;
  }

  /**
   * Add a region from the head or tail to the List of regions to return.
   */
  private void addRegionPlan(final MinMaxPriorityQueue<RegionPlan> regionsToMove,
      final boolean fetchFromTail, final ServerName sn, List<RegionPlan> regionsToReturn) {
    RegionPlan rp = null;
    if (!fetchFromTail) rp = regionsToMove.remove();
    else rp = regionsToMove.removeLast();
    rp.setDestination(sn);
    regionsToReturn.add(rp);
  }

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
}
