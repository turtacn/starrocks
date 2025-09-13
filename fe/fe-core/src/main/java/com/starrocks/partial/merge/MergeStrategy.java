package com.starrocks.partial.merge;

import com.starrocks.transaction.VersionInfo;
import java.util.List;

public class MergeStrategy {
    private List<VersionInfo> baseVersions;
    private List<VersionInfo> incrementalVersions;
    private MergeType mergeType;

    public List<VersionInfo> getBaseVersions() {
        return baseVersions;
    }

    public void setBaseVersions(List<VersionInfo> baseVersions) {
        this.baseVersions = baseVersions;
    }

    public List<VersionInfo> getIncrementalVersions() {
        return incrementalVersions;
    }

    public void setIncrementalVersions(List<VersionInfo> incrementalVersions) {
        this.incrementalVersions = incrementalVersions;
    }

    public MergeType getMergeType() {
        return mergeType;
    }

    public void setMergeType(MergeType mergeType) {
        this.mergeType = mergeType;
    }
}
