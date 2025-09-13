package com.starrocks.partial.merge;

import com.starrocks.thrift.TTabletVersionInfo;
import java.util.List;

public class MergeStrategy {
    private List<TTabletVersionInfo> baseVersions;
    private List<TTabletVersionInfo> incrementalVersions;
    private MergeType mergeType;

    public List<TTabletVersionInfo> getBaseVersions() {
        return baseVersions;
    }

    public void setBaseVersions(List<TTabletVersionInfo> baseVersions) {
        this.baseVersions = baseVersions;
    }

    public List<TTabletVersionInfo> getIncrementalVersions() {
        return incrementalVersions;
    }

    public void setIncrementalVersions(List<TTabletVersionInfo> incrementalVersions) {
        this.incrementalVersions = incrementalVersions;
    }

    public MergeType getMergeType() {
        return mergeType;
    }

    public void setMergeType(MergeType mergeType) {
        this.mergeType = mergeType;
    }
}
