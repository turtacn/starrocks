package com.starrocks.test.util;

import com.starrocks.partial.TabletStatus;
import java.util.HashMap;
import java.util.Map;

public class MockFrontendService {
    private Map<Long, TabletStatus> tabletStatuses = new HashMap<>();

    public TabletStatus getTabletStatus(long tabletId) {
        return tabletStatuses.getOrDefault(tabletId, TabletStatus.HEALTHY);
    }

    public void updateTabletStatus(long tabletId, TabletStatus status) {
        tabletStatuses.put(tabletId, status);
    }
}
