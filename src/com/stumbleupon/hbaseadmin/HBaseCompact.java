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

import java.util.Set;
import java.util.List;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.ipc.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.stringparsers.DateStringParser;
import com.martiansoftware.jsap.JSAPResult;

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
  private static void compactAllServers(int throttleFactor) throws Exception {
    Set<String> server_set = ClusterUtils.getServers();
    String startHHmm = Utils.dateString(config.getDate("start_time"), "HHmm");
    String stopHHmm = Utils.dateString(config.getDate("end_time"), "HHmm");

    while (!server_set.isEmpty()) {
      HRegionInfo region = null;

      try {
        for (final String hostport: server_set) {

          region = ClusterUtils.getNextRegion(hostport,
                                              throttleFactor);
          Utils.waitTillTime(startHHmm, stopHHmm, sleep_between_checks);

          try {
            if (region != null) {
              log.info("Compacting: " + region.getRegionNameAsString() +
                       " on server " + hostport);
              admin.majorCompact(region.getRegionNameAsString());
            }
          } catch (TableNotFoundException ex) {
            log.warn("Could not compact: " + ex);
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
   * command line parsing, this will most likely be replaced by fetching
   * these values from some property file, instead of from the command line.
   * This is purely for the stand-alone version.
   */
  private static JSAP prepCmdLineParser()
    throws Exception {
    final JSAP jsap = new JSAP();

    final FlaggedOption site_xml = new FlaggedOption("hbase_site")
      .setStringParser(JSAP.STRING_PARSER)
      .setDefault("")
      .setRequired(true)
      .setShortFlag('c')
      .setLongFlag(JSAP.NO_LONGFLAG);
    site_xml.setHelp("Path to hbase-site.xml.");
    jsap.registerParameter(site_xml);

    final FlaggedOption throttle_factor = new FlaggedOption("throttle_factor")
      .setStringParser(JSAP.INTEGER_PARSER)
      .setDefault("1")
      .setRequired(false)
      .setShortFlag('t')
      .setLongFlag(JSAP.NO_LONGFLAG);
    site_xml.setHelp("Throttle factor to limit the compaction queue.  The default (1) limits it to num threads / 1");
    jsap.registerParameter(throttle_factor);

    final FlaggedOption num_cycles = new FlaggedOption("num_cycles")
      .setStringParser(JSAP.INTEGER_PARSER)
      .setDefault("1")
      .setRequired(false)
      .setShortFlag('n')
      .setLongFlag(JSAP.NO_LONGFLAG);
    site_xml.setHelp("Number of iterations to run.  The default is 1.  Set to 0 to run forever.");
    jsap.registerParameter(num_cycles);

    final FlaggedOption pause_interval = new FlaggedOption("pause_interval")
      .setStringParser(JSAP.INTEGER_PARSER)
      .setDefault("30000")
      .setRequired(false)
      .setShortFlag('p')
      .setLongFlag(JSAP.NO_LONGFLAG);
    site_xml.setHelp("Time (in milliseconds) to pause between compactions.");
    jsap.registerParameter(pause_interval);

    final FlaggedOption wait_interval = new FlaggedOption("wait_interval")
      .setStringParser(JSAP.INTEGER_PARSER)
      .setDefault("60000")
      .setRequired(false)
      .setShortFlag('w')
      .setLongFlag(JSAP.NO_LONGFLAG);
    site_xml.setHelp("Time (in milliseconds) to wait between " +
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
    site_xml.setHelp("Time to start compactions.");
    jsap.registerParameter(start_time);

    final FlaggedOption end_time = new FlaggedOption("end_time")
      .setStringParser(date_parser)
      .setDefault("07:00")
      .setRequired(true)
      .setShortFlag('e')
      .setLongFlag(JSAP.NO_LONGFLAG);
    site_xml.setHelp("Time to stop compactions.");
    jsap.registerParameter(end_time);

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

    final String startHHmm = Utils.dateString(config.getDate("start_time"), "HHmm");
    final String stopHHmm = Utils.dateString(config.getDate("end_time"), "HHmm");

    if (num_cycles == 0)
      iteration = -1;

    while (iteration < num_cycles) {
      Utils.waitTillTime(startHHmm, stopHHmm, sleep_between_checks);
      ClusterUtils.updateStatus(admin);
      compactAllServers(throttle_factor);
      if (num_cycles > 0)
        ++iteration;
    }
  }
}
