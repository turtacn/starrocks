package com.starrocks.clone;

import java.util.HashSet;
import java.util.Set;

public class MockTabletScheduler extends TabletScheduler {
    private Set<Long> scheduledTablets = new HashSet<>();

    public MockTabletScheduler(TabletSchedulerStat stat) {
        super(stat);
    }

    // This is a mock method for testing purposes.
    public void scheduleTabletRecovery(long tabletId) {
        scheduledTablets.add(tabletId);
    }

    // This is a mock method for testing purposes.
    public boolean isScheduled(long tabletId) {
        return scheduledTablets.contains(tabletId);
    }
}
