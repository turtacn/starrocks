package com.starrocks.partial.merge;

import java.util.List;

public class MergeStrategy {
    private MergeType mergeType;
    private List<Long> baseVersions;
    private List<Long> incrementalVersions;

    public List<Long> getBaseVersions() {
        return baseVersions;
    }

    public void setBaseVersions(List<Long> baseVersions) {
        this.baseVersions = baseVersions;
    }

    public List<Long> getIncrementalVersions() {
        return incrementalVersions;
    }

    public void setIncrementalVersions(List<Long> incrementalVersions) {
        this.incrementalVersions = incrementalVersions;
    }

    public MergeType getMergeType() {
        return mergeType;
    }

    public void setMergeType(MergeType mergeType) {
        this.mergeType = mergeType;
    }
}
