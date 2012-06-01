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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Handles offline HBase compactions - runs compactions between pre-set times.
 */
public class HBaseCompact implements Callable<Integer> {

  private final static Logger log = LoggerFactory.getLogger(HBaseCompact.class);
  //Initialize the HBase config.

  private int throttleFactor;
  private int numCycles;
  private int sleepBetweenCompacts;
  private int sleepBetweenChecks;
  private Set<String> tableNames = null;

  private HBaseAdmin admin;
  private boolean dryRun = true;
  private String jmxremote_password;
  private Date startTime;
  private Date stopTime;
  private int filesKeep;


  public HBaseCompact setThrottleFactor(int throttleFactor) {
    this.throttleFactor = throttleFactor;
    return this;
  }

  public HBaseCompact setNumCycles(int num_cycles) {
    this.numCycles = num_cycles;
    return this;
  }

  public HBaseCompact setSleepBetweenCompacts(int sleep_between_compacts) {
    this.sleepBetweenCompacts = sleep_between_compacts;
    return this;
  }

  public HBaseCompact setSleepBetweenChecks(int sleep_between_checks) {
    this.sleepBetweenChecks = sleep_between_checks;
    return this;
  }

  public HBaseCompact setTableNames(String[] table_names) {
    if (null != this.tableNames) {
      this.tableNames.clear();
    } else {
      this.tableNames = new HashSet<String>();
    }
    if(null != table_names) {
      for (String table_name : table_names) {
        this.tableNames.add(table_name);
      }
    }
    return this;
  }

  public HBaseCompact setAdmin(HBaseAdmin admin) {
    this.admin = admin;
    return this;
  }

  public HBaseCompact setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
    return this;
  }

  public HBaseCompact setJmxPassword(String jmxremote_password) {
    this.jmxremote_password = jmxremote_password;
    return this;
  }

  public HBaseCompact setStartTime(Date startTime) {
    this.startTime = startTime;
    return this;
  }

  private HBaseCompact setStopTime(Date endTime) {
    this.stopTime = endTime;
    return this;
  }

  public HBaseCompact() {

  }


  /**
   * command line parsing, this will most likely be replaced by fetching these values from some property file, instead
   * of from the command line. This is purely for the stand-alone version.
   */
  private static JSAP prepCmdLineParser()
      throws Exception {
    final JSAP jsap = new JSAP();

    final FlaggedOption site_xml = new FlaggedOption("hbase_site")
        .setStringParser(JSAP.STRING_PARSER)
        .setDefault("/etc/hbase/conf/hbase-site.xml")
        .setRequired(true)
        .setShortFlag('c')
        .setLongFlag(JSAP.NO_LONGFLAG);
    site_xml.setHelp("Path to hbase-site.xml");
    jsap.registerParameter(site_xml);

    final FlaggedOption jmxremote_password = new FlaggedOption("jmxremote_password")
        .setStringParser(JSAP.STRING_PARSER)
        .setDefault("/etc/hbase/conf/jmxremote.password")
        .setRequired(true)
        .setShortFlag('j')
        .setLongFlag(JSAP.NO_LONGFLAG);
    jmxremote_password.setHelp("Path to jmxremote.password.");
    jsap.registerParameter(jmxremote_password);

    final FlaggedOption throttleFactor = new FlaggedOption("throttleFactor")
        .setStringParser(JSAP.INTEGER_PARSER)
        .setDefault("1")
        .setRequired(false)
        .setShortFlag('t')
        .setLongFlag(JSAP.NO_LONGFLAG);
    throttleFactor.setHelp("Throttle factor to limit the compaction queue.  The default (1) limits it to num threads / 1");
    jsap.registerParameter(throttleFactor);

    final FlaggedOption num_cycles = new FlaggedOption("numCycles")
        .setStringParser(JSAP.INTEGER_PARSER)
        .setDefault("1")
        .setRequired(false)
        .setShortFlag('n')
        .setLongFlag(JSAP.NO_LONGFLAG);
    num_cycles.setHelp("Number of iterations to run.  The default is 1.  Set to 0 to run forever.");
    jsap.registerParameter(num_cycles);

    final FlaggedOption pauseInterval = new FlaggedOption("pauseInterval")
        .setStringParser(JSAP.INTEGER_PARSER)
        .setDefault("30000")
        .setRequired(false)
        .setShortFlag('p')
        .setLongFlag(JSAP.NO_LONGFLAG);
    pauseInterval.setHelp("Time (in milliseconds) to pause between compactions.");
    jsap.registerParameter(pauseInterval);

    final FlaggedOption waitInterval = new FlaggedOption("waitInterval")
        .setStringParser(JSAP.INTEGER_PARSER)
        .setDefault("60000")
        .setRequired(false)
        .setShortFlag('w')
        .setLongFlag(JSAP.NO_LONGFLAG);
    waitInterval.setHelp("Time (in milliseconds) to wait between " +
        "time (are we there yet?) checks.");
    jsap.registerParameter(waitInterval);

    DateStringParser date_parser = DateStringParser.getParser();
    date_parser.setProperty("format", "HH:mm");

    final FlaggedOption startTime = new FlaggedOption("startTime")
        .setStringParser(date_parser)
        .setDefault("01:00")
        .setRequired(true)
        .setShortFlag('s')
        .setLongFlag(JSAP.NO_LONGFLAG);
    startTime.setHelp("Time to start compactions.");
    jsap.registerParameter(startTime);

    final FlaggedOption endTime = new FlaggedOption("endTime")
        .setStringParser(date_parser)
        .setDefault("07:00")
        .setRequired(true)
        .setShortFlag('e')
        .setLongFlag(JSAP.NO_LONGFLAG);
    endTime.setHelp("Time to stop compactions.");
    jsap.registerParameter(endTime);

    final FlaggedOption dryRun = new FlaggedOption("dryRun")
        .setStringParser(JSAP.BOOLEAN_PARSER)
        .setDefault("false")
        .setRequired(false)
        .setShortFlag('d')
        .setLongFlag(JSAP.NO_LONGFLAG);
    dryRun.setHelp("Don't actually do any compactions.");
    jsap.registerParameter(dryRun);

    final FlaggedOption table_names = new FlaggedOption("tableNames")
        .setStringParser(JSAP.STRING_PARSER)
        .setRequired(false)
        .setShortFlag(JSAP.NO_SHORTFLAG)
        .setLongFlag("tableNames")
        .setList(true)
        .setListSeparator(',');
    table_names.setHelp("Specific table names to check against (default is all)");
    jsap.registerParameter(table_names);

    final FlaggedOption files_keep = new FlaggedOption("filesKeep")
            .setStringParser(JSAP.INTEGER_PARSER)
            .setRequired(false)
            .setShortFlag('f')
            .setLongFlag("filesKeep")
            .setDefault("5");
    files_keep.setHelp("Number of storefiles to look for before compacting (default is 5)");
    jsap.registerParameter(files_keep);


    return jsap;
  }


  public static void main(String[] args) throws Exception {
    //parse command line args
    final JSAP jsap = prepCmdLineParser();
    final Configuration hbase_conf = HBaseConfiguration.create();

    JSAPResult config = jsap.parse(args);

    if (!config.success()) {
      System.err.println("Usage: java " +
          HBaseCompact.class.getName() +
          " " + jsap.getUsage());
      System.exit(-1);
    }

    hbase_conf.addResource(new Path(config.getString("hbase_site")));

    HBaseCompact compact = new HBaseCompact()
        .setSleepBetweenCompacts(config.getInt("pauseInterval"))
        .setSleepBetweenChecks(config.getInt("waitInterval"))
        .setThrottleFactor(config.getInt("throttleFactor"))
        .setNumCycles(config.getInt("numCycles"))
        .setAdmin(new HBaseAdmin(hbase_conf))
        .setTableNames(config.getStringArray("tableNames"))
        .setDryRun(config.getBoolean("dryRun"))
        .setJmxPassword(config.getString("jmxremote_password"))
        .setStartTime(config.getDate("startTime"))
        .setStopTime(config.getDate("endTime"))
        .setFilesKeep(config.getInt("filesKeep"));

    int pendingActions = compact.call();
    System.exit(pendingActions);
  }

  /**
   * Main compaction loop.
   */
  @Override
  public Integer call() {
    int iteration = 0;

    final String startHHmm = Utils.dateString(startTime, "HHmm");
    final String stopHHmm = Utils.dateString(stopTime, "HHmm");

    if (numCycles == 0)
      iteration = -1;
    try {

      if (admin.getConfiguration().get("hbase.hregion.majorcompaction").equals("0")) {
        while (iteration < numCycles) {
          //this try catch is so the tool doesn't die if constantly running,
          //it just restarts from scratch on a new iteration
          try {
            ClusterRegionStatus crs = new ClusterRegionStatus(admin, jmxremote_password, tableNames);
            if (dryRun) {
              int actionsPending = debugCurrentActions(crs);
              log.info("exiting after first run");
              return actionsPending;
            }
            int actionsAttempted = 0;
            for (ServerName name : crs.getServers()) {
              if (isLoadAcceptable(name)) {
                actionsAttempted += executeActionPerServer(crs, name);
              }
            }
            if (actionsAttempted == 0) {
              log.info("No more region actions to attempt, sleeping for " + this.sleepBetweenChecks);
              Thread.sleep(sleepBetweenChecks);
              crs.refreshRegionStatus();
            }
          } catch (IOException ioe) {
            log.error("IOException caught : ", ioe);
          } catch (RuntimeException re) {
            log.error("RuntimeException caught : ", re);
          } catch (Exception e) {
            log.error("Exception caught : ", e);
          }
        }
      } else {
        log.info("Automatic compactions are on for this cluster");
      }
      return 0;
    } finally {
      try {
        admin.close();
      } catch (IOException e) {
        String errorMsg = String.format("Encountered exception of type %s", e.getClass().getSimpleName());
        log.error(errorMsg, e);
        e.printStackTrace();
        throw new RuntimeException(errorMsg, e);
      }
    }
  }

  /**
   * Search for the first action that can be executed for a given server
   *
   * @param regionStatuses
   * @param name
   * @return
   * @throws Exception
   */
  private int executeActionPerServer(ClusterRegionStatus regionStatuses, ServerName name) throws Exception {
    ClusterRegionStatus.RegionSizeStatus regionSizeStatus = regionStatuses.getNextServerRegionStatus(name);
    while (null != regionSizeStatus) {
      CompactAction action = getAction(regionSizeStatus);
      if (action.getType() != CompactActionType.NONE) {
        action.execute();
        log.info("Done performing compaction, waiting for " + sleepBetweenCompacts + " before moving on");
        Thread.sleep(sleepBetweenCompacts);
        return 1;
      } else {
        regionSizeStatus = regionStatuses.getNextServerRegionStatus(name);
      }
    }
    return 0;
  }

  /**
   * Prints out all the actions that are pending on a cluster
   * @param crs
   */
  private int debugCurrentActions(ClusterRegionStatus crs) {
    Map<ServerName, List<CompactAction>> serverCompactActions = getAllActions(crs);
    int totalActions = 0;
    for(ServerName name : serverCompactActions.keySet()) {
      log.info("Printing pending compaction actions for server " + name.getHostname());
      for(CompactAction action : serverCompactActions.get(name)) {
        totalActions++;
        log.info("Want to perform " + action);
      }
    }
    return totalActions;
  }

  /**
   * Iterators over all statuses in a ClusterRegionStatus for discovering the pending actions
   * @param crs
   * @return
   */
  private Map<ServerName, List<CompactAction>> getAllActions(ClusterRegionStatus crs) {
    Map<ServerName,List<CompactAction>> perServerActions = new HashMap<ServerName,List<CompactAction>>();
    for(ServerName name : crs.getServers()) {
      List<CompactAction> actions = new ArrayList<CompactAction>();
      ClusterRegionStatus.RegionSizeStatus status = crs.getNextServerRegionStatus(name);
      while(null != status) {
        CompactAction action = getAction(status);
        if(action.getType() != CompactActionType.NONE)
          actions.add(action);
        status = crs.getNextServerRegionStatus(name);
      }
      perServerActions.put(name,actions);
    }
    return perServerActions;
  }

  /**
   * Returns true iff the load is low enough to commit an action based on current throttling sizes
   *
   * @param name
   * @return
   */
  private boolean isLoadAcceptable(ServerName name) throws Exception {
    final int compactionQueueSize =
        queryJMXIntValue(name.getHostname() + ":" + "10102",
            "hadoop:name=RegionServerStatistics,service=RegionServer",
            "compactionQueueSize",
            jmxremote_password);
    final int cpuSize = queryJMXIntValue(name.getHostname() + ":" + "10102",
        "java.lang:type=OperatingSystem",
        "AvailableProcessors", jmxremote_password);
    return compactionQueueSize < (cpuSize / throttleFactor);
  }

  /**
   * Helper to print the
   * @param hostport
   * @param mbean
   * @param command
   * @param password_file
   * @return
   * @throws Exception
   */
  public static int queryJMXIntValue(String hostport,
                                     String mbean,
                                     String command,
                                     String password_file) throws Exception {
    final JMXQuery client = new JMXQuery(mbean, command, password_file);
    return Integer.parseInt(client.execute(hostport));
  }

  /**
   * Determines the desired compaction action for a region based on the current region status
   *
   * @param regionSizeStatus
   * @return
   */
  private CompactAction getAction(ClusterRegionStatus.RegionSizeStatus regionSizeStatus) {
    HRegionInfo info = regionSizeStatus.getRegionInfo();
    HRegionInterface hri = regionSizeStatus.getRegionInterface();
    // We expect to have expected store file count for each family
    int expectedStoreFiles = filesKeep * regionSizeStatus.getFamilyCount();

      // Check if we have greater than the expected number of store files
    if (regionSizeStatus.getStoreFileCount() > expectedStoreFiles) {
      log.debug("Need to compact: " + info.getRegionNameAsString() + " because number of store files is " + regionSizeStatus.getStoreFileCount());
      return new CompactAction(CompactActionType.COMPACT, info, hri);
    } else {
      return new CompactAction(CompactActionType.NONE, info, hri);
    }
  }

  public HBaseCompact setFilesKeep(int filesKeep) {
    this.filesKeep = filesKeep;
    return this;
  }

  public enum CompactActionType {NONE, COMPACT}

  private class CompactAction {
    private CompactActionType type = CompactActionType.NONE;
    private HRegionInfo info;
    private HRegionInterface hri;

    public CompactActionType getType() {
      return type;
    }

    public HRegionInfo getInfo() {
      return info;
    }

    private CompactAction(CompactActionType type, HRegionInfo info, HRegionInterface hri) {

      this.type = type;
      this.info = info;
      this.hri = hri;
    }

    private void execute() throws InterruptedException, IOException {
      if (type == CompactActionType.COMPACT) {
        log.info("Compacting region " + info.getRegionNameAsString());
        hri.compactRegion(info, true);
      }
    }

    @Override
    public String toString() {
      return type.toString() + " on " + info.getRegionNameAsString();
    }
  }
}



