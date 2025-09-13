package com.starrocks.partial.merge;

import com.starrocks.transaction.VersionInfo;
import java.util.ArrayList;
import java.util.List;

public class MergeResult {
    private MergeStatus status;
    private List<VersionInfo> mergedVersions = new ArrayList<>();
    private String error;

    public void addMergedVersion(VersionInfo version) {
        this.mergedVersions.add(version);
    }

    public MergeStatus getStatus() {
        return status;
    }

    public void setStatus(MergeStatus status) {
        this.status = status;
    }

    public List<VersionInfo> getMergedVersions() {
        return mergedVersions;
    }

    public void setMergedVersions(List<VersionInfo> mergedVersions) {
        this.mergedVersions = mergedVersions;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
