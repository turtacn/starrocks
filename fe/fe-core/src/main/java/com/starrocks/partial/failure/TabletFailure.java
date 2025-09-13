package com.starrocks.partial.failure;

import com.google.gson.annotations.SerializedName;
import com.starrocks.partial.CleanupStatus;
import com.starrocks.partial.TabletStatus;

public class TabletFailure {
    @SerializedName("tabletId")
    private long tabletId;
    @SerializedName("backendId")
    private long backendId;
    @SerializedName("partitionId")
    private long partitionId;
    @SerializedName("tableId")
    private long tableId;
    @SerializedName("failTime")
    private long failTime;
    @SerializedName("recoveryTime")
    private Long recoveryTime;
    @SerializedName("status")
    private TabletStatus status;
    @SerializedName("tempTabletId")
    private Long tempTabletId;
    @SerializedName("cleanupStatus")
    private CleanupStatus cleanupStatus;
    @SerializedName("cleanupTime")
    private Long cleanupTime;
    @SerializedName("cleanupError")
    private String cleanupError;

    public long getTabletId() {
        return tabletId;
    }

    public void setTabletId(long tabletId) {
        this.tabletId = tabletId;
    }

    public long getBackendId() {
        return backendId;
    }

    public void setBackendId(long backendId) {
        this.backendId = backendId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(long partitionId) {
        this.partitionId = partitionId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public long getFailTime() {
        return failTime;
    }

    public void setFailTime(long failTime) {
        this.failTime = failTime;
    }

    public Long getRecoveryTime() {
        return recoveryTime;
    }

    public void setRecoveryTime(Long recoveryTime) {
        this.recoveryTime = recoveryTime;
    }

    public TabletStatus getStatus() {
        return status;
    }

    public void setStatus(TabletStatus status) {
        this.status = status;
    }

    public Long getTempTabletId() {
        return tempTabletId;
    }

    public void setTempTabletId(Long tempTabletId) {
        this.tempTabletId = tempTabletId;
    }

    public CleanupStatus getCleanupStatus() {
        return cleanupStatus;
    }

    public void setCleanupStatus(CleanupStatus cleanupStatus) {
        this.cleanupStatus = cleanupStatus;
    }

    public Long getCleanupTime() {
        return cleanupTime;
    }

    public void setCleanupTime(Long cleanupTime) {
        this.cleanupTime = cleanupTime;
    }

    public String getCleanupError() {
        return cleanupError;
    }

    public void setCleanupError(String cleanupError) {
        this.cleanupError = cleanupError;
    }
}
