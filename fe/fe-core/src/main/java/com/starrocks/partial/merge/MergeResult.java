package com.starrocks.partial.merge;

import java.util.ArrayList;
import java.util.List;

public class MergeResult {
    private MergeStatus status;
    private String error;
    private List<Long> mergedVersions = new ArrayList<>();

    public void addMergedVersion(Long version) {
        mergedVersions.add(version);
    }

    public MergeStatus getStatus() {
        return status;
    }

    public void setStatus(MergeStatus status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public List<Long> getMergedVersions() {
        return mergedVersions;
    }

    public void setMergedVersions(List<Long> mergedVersions) {
        this.mergedVersions = mergedVersions;
    }
}
