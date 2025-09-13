package com.starrocks.partial.rpc;

import java.util.ArrayList;
import java.util.List;

public class CleanupTempTabletsRequest {
    private List<Long> tempTabletIds = new ArrayList<>();

    public void addTempTabletId(long tabletId) {
        this.tempTabletIds.add(tabletId);
    }

    public List<Long> getTempTabletIds() {
        return tempTabletIds;
    }
}
