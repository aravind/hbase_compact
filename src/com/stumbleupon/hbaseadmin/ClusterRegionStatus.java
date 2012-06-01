package com.stumbleupon.hbaseadmin;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Created by IntelliJ IDEA. User: derek Date: 5/1/12 Time: 12:23 PM
 */
public class ClusterRegionStatus {

  private static Logger log = Logger.getLogger(ClusterRegionStatus.class);
  private HBaseAdmin admin;
  private Map<ServerName, LinkedList<RegionSizeStatus>> serverMap;
  private Set<String> tables_enabled;
  private ClusterStatus status;

  public ClusterRegionStatus(HBaseAdmin admin, String jmxremote_password, Set<String> tables_enabled) {
    this.admin = admin;
    this.tables_enabled = tables_enabled;
    refreshRegionStatus();
  }


  private boolean isRegionEnabled(HRegionInfo region_info) {
    if (tables_enabled.size() == 0) {
      return true;
    } else {
      return tables_enabled.contains(new String(region_info.getTableName()));
    }
  }

  public void refreshRegionStatus() {
    refreshClusterStatus();
    if (null == serverMap)
      serverMap = new HashMap<ServerName, LinkedList<RegionSizeStatus>>();
    else
      serverMap.clear();

    for (ServerName serverName : status.getServers()) {
      HServerLoad load = status.getLoad(serverName);
      try {
        HRegionInterface hri = admin.getConnection().getHRegionConnection(serverName.getHostname(), serverName.getPort());
        List<RegionSizeStatus> regions = new ArrayList<RegionSizeStatus>();
        for (HRegionInfo info : hri.getOnlineRegions()) {
          regions.add(getRegionStatus(info,hri,load));
        }
        serverMap.put(serverName,new LinkedList<RegionSizeStatus>(regions));
      } catch (IOException e) {
        String errorMsg = String.format("Encountered exception of type %s", e.getClass().getSimpleName());
        log.error(errorMsg, e);
        e.printStackTrace();
        throw new RuntimeException(errorMsg, e);
      }
    }
  }

  /**
   * Refresh cluster status data and server load information
   */
  private void refreshClusterStatus() {
    try {
      status = admin.getClusterStatus();
    } catch (IOException e) {
      String errorMsg = String.format("Encountered exception of type %s", e.getClass().getSimpleName());
      log.error(errorMsg, e);
      e.printStackTrace();
      throw new RuntimeException(errorMsg, e);
    }
  }

  private RegionSizeStatus getRegionStatus(HRegionInfo info, HRegionInterface hri, HServerLoad load) throws IOException {
    if (!isRegionEnabled(info)) {
      return new RegionSizeStatus(info,hri,0,0,0);
    }
    HServerLoad.RegionLoad regionLoad = load.getRegionsLoad().get(info.getRegionName());
    if (null == regionLoad) {
      log.error("Unable to find region load for region " + info.getRegionNameAsString());
      return new RegionSizeStatus(info,hri,0,0,0);
    }
    final int storeFileCount = regionLoad.getStorefiles();
    final long storeFileSize = regionLoad.getStorefileSizeMB();
    final AssignmentManager.RegionState stateForRegion = status.getRegionsInTransition().get(info.getRegionNameAsString());

    if (stateForRegion != null) {
      log.debug("Ignoring region status for region due to transition state: " + stateForRegion.toString());
      return new RegionSizeStatus(info,hri,0,0,0);
    }
    if (info.isOffline()) {
      log.debug("Offline, not splitting or compacting: " + info.toString());
      return new RegionSizeStatus(info,hri,0,0,0);
    }
    
    return new RegionSizeStatus(info,hri,storeFileCount,admin.getTableDescriptor(info.getTableName()).getColumnFamilies().length,storeFileSize);

  }

  public Collection<ServerName> getServers() {
    return serverMap.keySet();
  }

  public RegionSizeStatus getNextServerRegionStatus(ServerName name) {
    LinkedList<RegionSizeStatus> statuses = serverMap.get(name);
    if(statuses.size() == 0)
      return null;
    return statuses.remove();
  }

  /**
   * Refreshes current server load information and returns the current value of server load
   * @param name
   * @return
   */
  public HServerLoad getCurrentServerLoad(ServerName name) {
    refreshClusterStatus();
    return status.getLoad(name);
  }

  /**
   * Represents compaction status of a region
   * a size of 0 means that this region is not being considered for compaction at all
   */
  public class RegionSizeStatus {
    private HRegionInfo regionInfo;
    private HRegionInterface hri;
    private int storeFileCount;
    private int familyCount;
    private long storeFileSizeMB;

    public HRegionInfo getRegionInfo() {
      return regionInfo;
    }

    public int getStoreFileCount() {
      return storeFileCount;
    }

    public int getFamilyCount() {
      return familyCount;
    }

    public long getStoreFileSizeMB() {
      return storeFileSizeMB;
    }

    private RegionSizeStatus(HRegionInfo regionInfo, HRegionInterface hri, int storeFileCount, int familyCount, long storeFileSizeMB) {

      this.regionInfo = regionInfo;
      this.hri = hri;
      this.storeFileCount = storeFileCount;
      this.familyCount = familyCount;
      this.storeFileSizeMB = storeFileSizeMB;
    }

    public HRegionInterface getRegionInterface() {
      return this.hri;
    }
  }
}
