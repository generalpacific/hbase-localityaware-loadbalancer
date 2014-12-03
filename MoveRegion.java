/**
 *
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
package org.apache.hadoop.hbase;

import static org.codehaus.jackson.map.SerializationConfig.Feature.SORT_PROPERTIES_ALPHABETICALLY;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.MathContext;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterAllFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.MurmurHash;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This class will move regions from one server to another to unbalance the cluster.
 *
 */
public class MoveRegion extends Configured implements Tool {
  protected static final Log LOG = LogFactory.getLog(MoveRegion.class.getName());

  public static final String TABLE_NAME = "TestTable";
  public static final byte[] FAMILY_NAME = Bytes.toBytes("info");
  public static final byte[] QUALIFIER_NAME = Bytes.toBytes("data");
  public static final int VALUE_LENGTH = 1000;
  public static final int ROW_LENGTH = 26;

  private static final int ONE_GB = 1024 * 1024 * 1000;
  private static final int ROWS_PER_GB = ONE_GB / VALUE_LENGTH;

  /**
   * Enum for map metrics.  Keep it out here rather than inside in the Map
   * inner-class so we can find associated properties.
   */
  protected static enum Counter {
    /** elapsed time */
    ELAPSED_TIME,
    /** number of rows */
    ROWS
  }

  /**
   * Constructor
   * @param conf Configuration object
   */
  public MoveRegion(final Configuration conf) {
    super(conf);
  }

  /**
   * Implementations can have their status set.
   */
  interface Status {
    /**
     * Sets status
     * @param msg status message
     * @throws IOException
     */
    void setStatus(final String msg) throws IOException;
  }


  /**
   * Create an HTableDescriptor from provided TestOptions.
   */
  protected static HTableDescriptor getTableDescriptor(TestOptions opts) {
    HTableDescriptor desc = new HTableDescriptor(opts.tableName);
    HColumnDescriptor family = new HColumnDescriptor(FAMILY_NAME);
    family.setDataBlockEncoding(opts.blockEncoding);
    family.setCompressionType(opts.compression);
    family.setBloomFilterType(opts.bloomType);
    if (opts.inMemoryCF) {
      family.setInMemory(true);
    }
    desc.addFamily(family);
    return desc;
  }

  /**
   * Wraps up options passed to {@link org.apache.hadoop.hbase.MoveRegion}.
   * This makes tracking all these arguments a little easier.
   */
  static class TestOptions {
    public boolean filterAll = false;
    public int startRow = 0;
    public int perClientRunRows = ROWS_PER_GB;
    public String tableName = TABLE_NAME;
    public boolean flushCommits = true;
    boolean inMemoryCF = false;
    public Compression.Algorithm compression = Compression.Algorithm.NONE;
    public BloomType bloomType = BloomType.ROW;
    public DataBlockEncoding blockEncoding = DataBlockEncoding.NONE;
  }

  /*
   * A test.
   * Subclass to particularize what happens per row.
   */
  static abstract class Test {
    // Below is make it so when Tests are all running in the one
    // jvm, that they each have a differently seeded Random.
    private static final Random randomSeed = new Random(System.currentTimeMillis());
    private static long nextRandomSeed() {
      return randomSeed.nextLong();
    }
    protected final Random rand = new Random(nextRandomSeed());
    protected final Configuration conf;
    protected final TestOptions opts;

    private final Status status;
    protected HConnection connection;
    protected HTableInterface table;

    /**
     * Note that all subclasses of this class must provide a public contructor
     * that has the exact same list of arguments.
     */
    Test(final Configuration conf, final TestOptions options, final Status status) {
      this.conf = conf;
      this.opts = options;
      this.status = status;
    }

    private String generateStatus(final int sr, final int i, final int lr) {
      return sr + "/" + i + "/" + lr;
    }

    protected int getReportingPeriod() {
      int period = opts.perClientRunRows / 10;
      return period == 0 ? opts.perClientRunRows : period;
    }

    void testSetup() throws IOException {
      this.connection = HConnectionManager.createConnection(conf);
      this.table = connection.getTable(opts.tableName);
      this.table.setAutoFlush(false, true);
    }

    void testTakedown() throws IOException {
      if (opts.flushCommits) {
        this.table.flushCommits();
      }
      table.close();
      connection.close();
    }

    /*
     * Run test
     * @return Elapsed time.
     * @throws IOException
     */
    long test() throws IOException {
      testSetup();
      LOG.info("Timed test starting in thread " + Thread.currentThread().getName());
      final long startTime = System.nanoTime();
      try {
        testTimed();
      } finally {
        testTakedown();
      }
      return (System.nanoTime() - startTime) / 1000000;
    }

    /**
     * Provides an extension point for tests that don't want a per row invocation.
     */
    void testTimed() throws IOException {
      int lastRow = opts.startRow + opts.perClientRunRows;
      // Report on completion of 1/10th of total.
      for (int i = opts.startRow; i < lastRow; i++) {
        testRow(i);
        if (status != null && i > 0 && (i % getReportingPeriod()) == 0) {
          status.setStatus(generateStatus(opts.startRow, i, lastRow));
        }
      }
    }

    /*
    * Test for individual row.
    * @param i Row index.
    */
    abstract void testRow(final int i) throws IOException;
  }

  static class FilteredScanTest extends Test {
    protected static final Log LOG = LogFactory.getLog(FilteredScanTest.class.getName());

    FilteredScanTest(Configuration conf, TestOptions options, Status status) {
      super(conf, options, status);
    }

    @Override
    void testRow(int i) throws IOException {
      byte[] value = generateData(this.rand, VALUE_LENGTH);
      Scan scan = constructScan(value);
      ResultScanner scanner = null;
      try {
        scanner = this.table.getScanner(scan);
        while (scanner.next() != null) {
        }
      } finally {
        if (scanner != null) scanner.close();
      }
    }

    protected Scan constructScan(byte[] valuePrefix) throws IOException {
      FilterList list = new FilterList();
      Filter filter = new SingleColumnValueFilter(
              FAMILY_NAME, QUALIFIER_NAME, CompareFilter.CompareOp.EQUAL,
              new BinaryComparator(valuePrefix)
      );
      list.addFilter(filter);
      if(opts.filterAll) {
        list.addFilter(new FilterAllFilter());
      }
      Scan scan = new Scan();
      scan.addColumn(FAMILY_NAME, QUALIFIER_NAME);
      scan.setFilter(list);
      return scan;
    }
  }

  /*
   * Format passed integer.
   * @param number
   * @return Returns zero-prefixed ROW_LENGTH-byte wide decimal version of passed
   * number (Does absolute in case number is negative).
   */
  public static byte [] format(final int number) {
    byte [] b = new byte[ROW_LENGTH];
    int d = Math.abs(number);
    for (int i = b.length - 1; i >= 0; i--) {
      b[i] = (byte)((d % 10) + '0');
      d /= 10;
    }
    return b;
  }

  /*
   * This method takes some time and is done inline uploading data.  For
   * example, doing the mapfile test, generation of the key and value
   * consumes about 30% of CPU time.
   * @return Generated random value to insert into a table cell.
   */
  public static byte[] generateData(final Random r, int length) {
    byte [] b = new byte [length];
    int i = 0;

    for(i = 0; i < (length-8); i += 8) {
      b[i] = (byte) (65 + r.nextInt(26));
      b[i+1] = b[i];
      b[i+2] = b[i];
      b[i+3] = b[i];
      b[i+4] = b[i];
      b[i+5] = b[i];
      b[i+6] = b[i];
      b[i+7] = b[i];
    }

    byte a = (byte) (65 + r.nextInt(26));
    for(; i < length; i++) {
      b[i] = a;
    }
    return b;
  }

  /**
   * @deprecated Use {@link #generateData(java.util.Random, int)} instead.
   * @return Generated random value to insert into a table cell.
   */
  @Deprecated
  public static byte[] generateValue(final Random r) {
    return generateData(r, VALUE_LENGTH);
  }

  public int run(String[] args) throws Exception {
    int errCode = -1;

    int regionsToMove = 2;
    if (args.length == 1) {
      regionsToMove = Integer.parseInt(args[0]);
    }
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(getConf());
      ClusterStatus clusterStatus = admin.getClusterStatus();
      Collection<ServerName> serverNames = clusterStatus.getServers();
      System.err.println("+++++++++++++++++++++++++++++++++++++ regions to move = " + regionsToMove);
      System.err.println("+++++++++++++++++++++++++++++++++++++");
      System.err.println("Servers in cluster are : " + serverNames);
      List<Server> servers = new ArrayList<Server>();
      for (ServerName server : serverNames) {
        List<HRegionInfo> onlineRegions = admin.getOnlineRegions(server);
        Server server1 = new Server(server, onlineRegions);
        servers.add(server1);
        System.err.println("Server: " + server1);
      }
      Collections.sort(servers);
      System.err.println("Sorted servers: " + servers);

      int n = servers.size();
      int mid = n / 2;
      for (int i = 0, k = mid; i < mid && k < n; ++i, ++k) {
        Server server = servers.get(i);
        List<HRegionInfo> regions = server.getRegions();
        if (regions == null || regions.size() == 0) {
          continue;
        }
        int j = 1;
        Server destinationServer = servers.get(k);
        for (HRegionInfo region : regions) {
      //    System.err.println("Moving (" + region.getRegionName() +") to (" + destinationServer.getServerName() + ")");
          admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(destinationServer.getServerName().getServerName()));
          if (j >= regionsToMove) {
            break;
          }
          ++j;
        }
      }
      System.err.println("################### AFTER MOVING REGIONS ###############");
      serverNames = admin.getClusterStatus().getServers();
      for (ServerName server : serverNames) {
        List<HRegionInfo> onlineRegions = admin.getOnlineRegions(server);
        System.err.println("Server: " + new Server(server, onlineRegions));
      }
      System.err.println("+++++++++++++++++++++++++++++++++++++");
    } finally {
      if (admin != null) admin.close();
    }
    return errCode;
  }

  public static void main(final String[] args) throws Exception {
    int res = ToolRunner.run(new MoveRegion(HBaseConfiguration.create()), args);
    System.exit(res);
  }

  class Server implements  Comparable<Server> {

    private ServerName serverName;
    private List<HRegionInfo> regions;

    public Server(ServerName serverName, List<HRegionInfo> regions) {
      this.serverName = serverName;
      this.regions = regions;
    }

    public ServerName getServerName() {
      return serverName;
    }

    public List<HRegionInfo> getRegions() {
      return regions;
    }

    @Override
    public int compareTo(Server server) {
      if (regions == null) {
        return 1;
      }
      if (server.regions == null) {
        return -1;
      }
      if (server.regions.size() < regions.size()) {
        return 1;
      }
      return -1;
    }

    @Override
    public String toString() {
      int numberOfRegions = regions == null ? 0 : regions.size();
      return serverName.getServerName() + ":" + numberOfRegions;
    }
  }
}
