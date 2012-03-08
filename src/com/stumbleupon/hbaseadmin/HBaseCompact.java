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

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.stringparsers.DateStringParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Handles offline HBase compactions - runs compactions between
 * pre-set times.
 */
public class HBaseCompact {

  private final static Logger log = LoggerFactory.getLogger(HBaseCompact.class);
  //Initialize the HBase config.

  private static int throttle_factor;
  private static int num_cycles;
  private static int sleep_between_compacts;
  private static int sleep_between_checks;

  private static HBaseAdmin admin;
  private static JSAPResult config;


  /**
   * cycles through all the servers and compacts regions.  We process one
   * region on each server, delete it from the region list, compact it, move
   * on to the next server and so on.  Once we are done with a server sweep
   * across all of them, we start over and repeat utill we are done with the
   * regions.
   */
  private static void compactAllServers(int throttleFactor, final String jmx_password) throws Exception {
    Set<String> server_set = ClusterUtils.getServers();
    String startHHmm = Utils.dateString(config.getDate("start_time"), "HHmm");
    String stopHHmm = Utils.dateString(config.getDate("end_time"), "HHmm");

    while (!server_set.isEmpty()) {
      Pair<HRegionInfo, HServerLoad.RegionLoad> region = null;

      try {
        for (final String hostport: server_set) {

          region = ClusterUtils.getNextRegion(hostport,
                                              throttleFactor,
                                              jmx_password);
          if (region != null) {
            HRegionInfo region_info = region.getFirst();
            HServerLoad.RegionLoad region_load = region.getSecond();
            Utils.waitTillTime(startHHmm, stopHHmm, sleep_between_checks);

            if (region_info != null && region_load != null) {
              splitOrCompactHRegion(region_info, region_load, admin, hostport);
            }
          }
        }

        Thread.sleep(sleep_between_compacts);
        server_set = ClusterUtils.getServers();

      } catch (RemoteException ex) {
        log.warn("Failed compaction for: " + region + ", Exception thrown: " + ex);
      }
    }
  }

  /**
   * Performs maintenance on a specific region. Currently is only able to decide to do a split or a compact, but
   * this can be modified to have configurable behaviors.
   *
   * The current logic for running a split/compact is
   * split if a regions store file size is greater than max_split_size_in_MB
   * compact if the number of store files is greater than the number of column families
   *
   * Currently does nothing if a region is found to be in transition
   *
   * @param region_info {@link HRegionInfo} for region on server eligible to be split/compacted
   * @param region_load {@link HServerLoad.RegionLoad} for region on server eligible to be split/compacted
   * @param admin {@link HBaseAdmin} for getting region state and split/compact
   * @param hostport host on which the region is
   * @throws IOException
   * @throws InterruptedException
   */
  private static void splitOrCompactHRegion(final HRegionInfo region_info, final HServerLoad.RegionLoad region_load, final HBaseAdmin admin, final String hostport) throws IOException, InterruptedException {
    final boolean dry_run = config.getBoolean("dry_run");
    final boolean do_splits = config.getBoolean("do_splits");
    //size at which a split is called
    final int max_split_size = config.getInt("max_split_size_in_MB");
    final String hregion_max_filesize = admin.getConfiguration().get("hbase.hregion.max.filesize");

    final int store_files = region_load.getStorefiles();
    final int store_file_size = region_load.getStorefileSizeMB();
    final AssignmentManager.RegionState state_for_region = admin.getClusterStatus().getRegionsInTransition().get(region_info.getRegionNameAsString());

    if (state_for_region != null) {
      log.info("Not doing anything for: " + state_for_region.toString());
      return;
    }

    try {

      log.info("Looking at region: " + region_info.getRegionNameAsString() +
          " on server " + hostport +
          " Stores: " + region_load.getStores() +
          " Store files: " + store_files +
          " Store file size: " + region_load.getStorefileSizeMB() +
          " Memstore size: " + region_load.getStorefileSizeMB()
      );

//      if (region_info.isRootRegion())
//      if (region_info.isMetaRegion())

      if (region_info.isOffline()) {
        log.info("Offline, not splitting or compacting: " + region_info.toString());
        return;
      }

      if ((store_file_size > max_split_size) && do_splits) {
        log.info("Trying to split: " + region_info.getRegionNameAsString() + " because store file size is " + store_file_size);
        if (!region_info.isSplitParent() && !region_info.isMetaRegion() && !region_info.isMetaTable() && !region_info.isRootRegion()) {
          log.info("Splitting: " + region_info.getRegionNameAsString() + " because store file size is " + store_file_size);
          log.info("Splitting: " + region_info.getRegionNameAsString() + " on server " + hostport);
          if (!dry_run)
            admin.split(region_info.getRegionNameAsString());
        }
        else {
          log.info("Not splitting: " + region_info.getRegionNameAsString());
        }
      }


      //todo: make configurable if we're temporarily ok with more store files/region
      //there is one store file per column family
      else if (store_files > region_info.getTableDesc().getColumnFamilies().length) {
        log.info("Compacting: " + region_info.getRegionNameAsString() + " because number of store files is " + store_files);
        log.info("Compacting: " + region_info.getRegionNameAsString() + " on server " + hostport);
        if (!dry_run)
          admin.majorCompact(region_info.getRegionNameAsString());
      }
      else {
        log.info("Not doing anything to: " + region_info.getRegionNameAsString());
      }
    } catch (IOException e) {
      log.warn("Caught IOException while compacting ", e);
      throw e;
    } catch (InterruptedException e) {
      log.warn("Caught InterruptedException while compacting ", e);
      throw e;
    }
  }


  /**
   * command line parsing, this will most likely be replaced by fetching
   * these values from some property file, instead of from the command line.
   * This is purely for the stand-alone version.
   */
  private static JSAP prepCmdLineParser()
    throws Exception {
    final JSAP jsap = new JSAP();

    final FlaggedOption site_xml = new FlaggedOption("hbase_site")
      .setStringParser(JSAP.STRING_PARSER)
      .setDefault("/etc/hbase/conf/hbase-site-xml")
      .setRequired(true)
      .setShortFlag('c')
      .setLongFlag(JSAP.NO_LONGFLAG);
    site_xml.setHelp("Path to hbase-site.xml.");
    jsap.registerParameter(site_xml);

    final FlaggedOption jmxremote_password = new FlaggedOption("jmxremote_password")
      .setStringParser(JSAP.STRING_PARSER)
      .setDefault("/etc/hbase/conf/jmxremote.password")
      .setRequired(true)
      .setShortFlag('j')
      .setLongFlag(JSAP.NO_LONGFLAG);
    jmxremote_password.setHelp("Path to jmxremote.password.");
    jsap.registerParameter(jmxremote_password);

    final FlaggedOption throttle_factor = new FlaggedOption("throttle_factor")
      .setStringParser(JSAP.INTEGER_PARSER)
      .setDefault("1")
      .setRequired(false)
      .setShortFlag('t')
      .setLongFlag(JSAP.NO_LONGFLAG);
    throttle_factor.setHelp("Throttle factor to limit the compaction queue.  The default (1) limits it to num threads / 1");
    jsap.registerParameter(throttle_factor);

    final FlaggedOption num_cycles = new FlaggedOption("num_cycles")
      .setStringParser(JSAP.INTEGER_PARSER)
      .setDefault("1")
      .setRequired(false)
      .setShortFlag('n')
      .setLongFlag(JSAP.NO_LONGFLAG);
    num_cycles.setHelp("Number of iterations to run.  The default is 1.  Set to 0 to run forever.");
    jsap.registerParameter(num_cycles);

    final FlaggedOption pause_interval = new FlaggedOption("pause_interval")
      .setStringParser(JSAP.INTEGER_PARSER)
      .setDefault("30000")
      .setRequired(false)
      .setShortFlag('p')
      .setLongFlag(JSAP.NO_LONGFLAG);
    pause_interval.setHelp("Time (in milliseconds) to pause between compactions.");
    jsap.registerParameter(pause_interval);

    final FlaggedOption wait_interval = new FlaggedOption("wait_interval")
      .setStringParser(JSAP.INTEGER_PARSER)
      .setDefault("60000")
      .setRequired(false)
      .setShortFlag('w')
      .setLongFlag(JSAP.NO_LONGFLAG);
    wait_interval.setHelp("Time (in milliseconds) to wait between " +
                     "time (are we there yet?) checks.");
    jsap.registerParameter(wait_interval);

    DateStringParser date_parser = DateStringParser.getParser();
    date_parser.setProperty("format", "HH:mm");

    final FlaggedOption start_time = new FlaggedOption("start_time")
      .setStringParser(date_parser)
      .setDefault("01:00")
      .setRequired(true)
      .setShortFlag('s')
      .setLongFlag(JSAP.NO_LONGFLAG);
    start_time.setHelp("Time to start compactions.");
    jsap.registerParameter(start_time);

    final FlaggedOption end_time = new FlaggedOption("end_time")
      .setStringParser(date_parser)
      .setDefault("07:00")
      .setRequired(true)
      .setShortFlag('e')
      .setLongFlag(JSAP.NO_LONGFLAG);
    end_time.setHelp("Time to stop compactions.");
    jsap.registerParameter(end_time);

    final FlaggedOption dry_run = new FlaggedOption("dry_run")
        .setStringParser(JSAP.BOOLEAN_PARSER)
        .setDefault("false")
        .setRequired(false)
        .setShortFlag('d')
        .setLongFlag(JSAP.NO_LONGFLAG);
    dry_run.setHelp("Don't actually do any compactions or splits.");
    jsap.registerParameter(dry_run);

    final FlaggedOption max_split_size = new FlaggedOption("max_split_size_in_MB")
        .setStringParser(JSAP.INTEGER_PARSER)
        .setDefault("256")
        .setRequired(false)
        .setShortFlag('m')
        .setLongFlag(JSAP.NO_LONGFLAG);
    max_split_size.setHelp("Maximum size for store files (in MB) at which a region is split.");
    jsap.registerParameter(max_split_size);

    final FlaggedOption do_splits = new FlaggedOption("do_splits")
        .setStringParser(JSAP.BOOLEAN_PARSER)
        .setDefault("false")
        .setRequired(false)
        .setShortFlag('h')
        .setLongFlag(JSAP.NO_LONGFLAG);
    do_splits.setHelp("Do splits (default split size will be 256MB unless specified).");
    jsap.registerParameter(do_splits);

    return jsap;
  }


  public static void main(String[] args) throws Exception {
    //parse command line args
    final JSAP jsap = prepCmdLineParser();
    final Configuration hbase_conf = HBaseConfiguration.create();
    int iteration = 0;

    config = jsap.parse(args);

    if (!config.success()) {
      System.err.println("Usage: java " +
                         HBaseCompact.class.getName() +
                         " " + jsap.getUsage());
    }

    hbase_conf.addResource(new Path(config.getString("hbase_site")));
    sleep_between_compacts = config.getInt("pause_interval");
    sleep_between_checks = config.getInt("wait_interval");
    throttle_factor = config.getInt("throttle_factor");
    num_cycles = config.getInt("num_cycles");
    admin = new HBaseAdmin(hbase_conf);
    String jmx_password = config.getString("jmxremote_password");
    final boolean dry_run = config.getBoolean("dry_run");

    final String startHHmm = Utils.dateString(config.getDate("start_time"), "HHmm");
    final String stopHHmm = Utils.dateString(config.getDate("end_time"), "HHmm");

    if (num_cycles == 0)
      iteration = -1;

    if (admin.getConfiguration().get("hbase.hregion.majorcompaction").equals("0")) {
      while (iteration < num_cycles) {
        try {
          Utils.waitTillTime(startHHmm, stopHHmm, sleep_between_checks);
          ClusterUtils.updateStatus(admin, jmx_password);
          compactAllServers(throttle_factor, jmx_password);
          if (num_cycles > 0)
            ++iteration;
        } catch (IOException ioe) {
          log.error("IOException caught : ", ioe);
        }
      }
    }
    else {
      log.info("Automatic compactions are on for this cluster");
    }
  }
}
