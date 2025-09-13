package com.starrocks.partial.merge;

import com.starrocks.thrift.TTabletVersionInfo;
import java.util.ArrayList;
import java.util.List;

public class MergeResult {
    private MergeStatus status;
    private List<TTabletVersionInfo> mergedVersions = new ArrayList<>();
    private String error;

    public void addMergedVersion(TTabletVersionInfo version) {
        this.mergedVersions.add(version);
    }

    public MergeStatus getStatus() {
        return status;
    }

    public void setStatus(MergeStatus status) {
        this.status = status;
    }

    public List<TTabletVersionInfo> getMergedVersions() {
        return mergedVersions;
    }

    public void setMergedVersions(List<TTabletVersionInfo> mergedVersions) {
        this.mergedVersions = mergedVersions;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
