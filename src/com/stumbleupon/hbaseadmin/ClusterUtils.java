/**
 * This file is part of hbaseadmin.
 * Copyright (C) 2011 StumbleUpon, Inc.
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. This program is distributed in the hope that it
 * will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy
 * of the GNU Lesser General Public License along with this program. If not,
 * see <http: *www.gnu.org/licenses/>.
 */

package com.stumbleupon.hbaseadmin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains the state of the cluster.
 */
class ClusterUtils {

  private final static Logger log = LoggerFactory.getLogger(ClusterUtils.class);

  //server -> regions map for regions yet to be compacted.
  private static HashMap<String, List<HRegionInfo>>
    s_region_map = new HashMap<String, List<HRegionInfo>>();

  //server -> cpu_count map for servers.
  private static HashMap<String, Integer> s_cpu_map = new HashMap<String,Integer>();

  private static HashMap<String, byte[]> slist = new HashMap<String,byte[]>();

  private static class RSCountCompare implements Comparator<String> {

    public int compare(final String hp1, final String hp2) {
      int nr1 = getNumRegions(hp1);
      int nr2 = getNumRegions(hp2);
      if ((nr1 > 0) && (nr2 > 0)) {
        if (nr1 > nr2) return 1;
        if (nr2 > nr1) return -1;
        return 0;
      } else if (nr1 > 0) {
        return 1;
      } else if (nr2 > 0) {
        return -1;
      }
      return 0;
    }
  }

  private final static Comparator<String>
    rsComparator = new ClusterUtils.RSCountCompare();


  public static Set<String>
    getServers() {
    return new HashSet(s_region_map.keySet());
  }


  /**
   * returns the next region to be processed and removes it from the list of
   * regions for the server.  When the server has no more regions, it's
   * removed from the sregions map.
   * sregions: map of server names and a list of regions on the server.
   */
  public static HRegionInfo
    getNextRegion(final String hostport,
                  final int throttleFactor) throws Exception {

    if (s_region_map.containsKey(hostport)) {
      if (!s_region_map.get(hostport).isEmpty()) {
        final String host = hostport.split(":")[0];
        final int compaction_queue_size =
          queryJMXIntValue(host + ":" + "10306",
                           "hadoop:name=RegionServerStatistics,service=RegionServer",
                           "compactionQueueSize");

        if (compaction_queue_size < (s_cpu_map.get(hostport).intValue() / throttleFactor))
          return s_region_map.get(hostport).remove(0);
        else
          log.warn(hostport + " has a queue size of " + compaction_queue_size +
                   ", skipping compaction for this round.");
      } else {
        // There are no more regions left, remove server.
        s_region_map.remove(hostport);
      }
    }
    return null;
  }


  public static int queryJMXIntValue(String hostport,
                                     String mbean,
                                     String command) throws Exception {
    final JMXQuery client = new JMXQuery(mbean, command);
    return (new Integer(client.execute(hostport))).intValue();
  }


  /**
   * re-fetches and populates the sregions and slist arrays.
   */
  public static HashMap<String, List<HRegionInfo>>
    updateStatus(final HBaseAdmin admin)
    throws Exception {
    final Configuration conf = admin.getConfiguration();
    final ClusterStatus cstatus = admin.getClusterStatus();
    final HashMap<String, List<HRegionInfo>>
      sregions = new HashMap<String,List<HRegionInfo>>();

    log.info("Fetching cluster status.");

    HConnection connection = admin.getConnection();

    s_region_map.clear();

    for (ServerName si: cstatus.getServers()) {
      try {
        final HRegionInterface hri =
          connection.getHRegionConnection(si.getHostname(), si.getPort());
        final String hostport = si.getHostAndPort();

        log.info("Querying: " + hostport);
        slist.put(hostport, si.getServerName().getBytes());
        sregions.put(hostport, hri.getOnlineRegions());
        s_region_map.put(hostport, hri.getOnlineRegions());
        s_cpu_map.put(hostport,
                      new Integer(queryJMXIntValue(si.getHostname() + ":" + "10306",
                                                   "java.lang:type=OperatingSystem",
                                                   "AvailableProcessors")));
      } catch (RetriesExhaustedException ex) {
        log.warn("Server down: " + si);
        log.warn("     Exception:" + ex);
      } catch (IOException ex) {
        log.warn("Problem querying server via JMX:" + si);
        log.warn("     Exception:" + ex);
      }
    }

    return sregions;
  }


  public static List<HRegionInfo>
    getRegionsOnServer(final String hostport,
                       final HashMap<String, List<HRegionInfo>> sregions) {

    if (sregions.containsKey(hostport))
      return sregions.get(hostport);

    return null;
  }


  private static int
    getNumRegions(final String hostport) {

    if (s_region_map.containsKey(hostport))
      return s_region_map.get(hostport).size();

    return -1;
  }


  public static String[]
    sortByRegionCount(final HashMap<String, List<HRegionInfo>> sregions) {
    String[] rs_array = sregions.keySet().toArray(new String[0]);

    Arrays.sort(rs_array, rsComparator);
    return rs_array;
  }


  // This is pretty much a java re-write of Stack's isSuccessfulScan
  public static boolean isRegionLive(final HBaseAdmin admin,
                                     final HRegionInfo region)
    throws Exception {
    final byte[] start_key = region.getStartKey();
    final Scan scan = new Scan(start_key);
    final byte[] table_name = HRegionInfo.
      getTableName(region.getRegionName());

    boolean return_status = false;
    HTable htable = null;
    ResultScanner scanner = null;

    scan.setBatch(1);
    scan.setCaching(1);
    scan.setFilter(new FirstKeyOnlyFilter());

    try {
      htable = new HTable(admin.getConfiguration(), table_name);

      if (htable != null) {
        scanner = htable.getScanner(scan);

        if (scanner != null) {
          scanner.next();
          return_status = true;
        }
      }

    } catch (IOException e) {

    } finally {

      if (scanner != null)
        scanner.close();
      if (htable != null)
        htable.close();
    }

    return return_status;
  }


  // This is pretty much a java re-write of Stack's getServerNameForRegion
  public static String
    getServerHostingRegion(final HBaseAdmin admin,
                           final HRegionInfo region) throws Exception {

    ServerName server = null;
    HConnection connection = admin.getConnection();

    if (region.isRootRegion()) {
      final RootRegionTracker tracker =
        new RootRegionTracker(connection.getZooKeeperWatcher(),
                              new Abortable() {
                                private boolean aborted = false;
                                public void abort(String why, Throwable e) {
                                  log.error("ZK problems: {}", why);
                                  aborted = true;
                                }

                                public boolean isAborted() {
                                  return aborted;
                                }
                                });
      tracker.start();
      while (!tracker.isLocationAvailable())
        Thread.sleep(100);
      server = tracker.getRootRegionLocation();
      tracker.stop();
      return (server.getHostname() + ":" + server.getPort());
    }

    HTable table;
    final Configuration conf = admin.getConfiguration();

    if (region.isMetaRegion()) {
      table = new HTable(conf, HConstants.ROOT_TABLE_NAME);
    } else {
      table = new HTable(conf, HConstants.META_TABLE_NAME);
    }

    Get get_request = new Get(region.getRegionName());

    get_request.addColumn(HConstants.CATALOG_FAMILY,
                          HConstants.SERVER_QUALIFIER);

    Result result = table.get(get_request);
    table.close();

    final byte[] servername = result.getValue(HConstants.CATALOG_FAMILY,
                             HConstants.SERVER_QUALIFIER);
    return Bytes.toString(servername);
  }


  public static boolean
    moveRegion(final HBaseAdmin admin,
               final HashMap<String, List<HRegionInfo>> sregions,
               final int index,
               final String shostport,
               final String dhostport) throws Exception {
    final HRegionInfo region_info = sregions.get(shostport).get(index);

    admin.move(region_info.getEncodedNameAsBytes(), slist.get(dhostport));

    if (isRegionLive(admin, region_info) &&
        dhostport.equals(getServerHostingRegion(admin,region_info))) {
      sregions.get(shostport).remove(index);
      sregions.get(dhostport).add(region_info);
      return true;
    }
    return false;
  }
}