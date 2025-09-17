package com.starrocks.partial.failure;

import com.starrocks.partial.CleanupStatus;
import com.starrocks.partial.TabletStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MockTabletFailureRepository implements TabletFailureRepository {
    private final Map<Long, TabletFailure> failureRecords = new HashMap<>();

    @Override
    public void save(TabletFailure tabletFailure) {
        // Create a copy to avoid external modifications to the object stored in the map
        failureRecords.put(tabletFailure.getTabletId(), new TabletFailure(tabletFailure));
    }

    @Override
    public TabletFailure findByTabletId(long tabletId) {
        TabletFailure record = failureRecords.get(tabletId);
        // Return a copy to prevent external modifications
        return record != null ? new TabletFailure(record) : null;
    }

    @Override
    public void deleteByTabletId(long tabletId) {
        failureRecords.remove(tabletId);
    }

    @Override
    public List<TabletFailure> getTabletsByStatus(TabletStatus status) {
        return failureRecords.values().stream()
                .filter(r -> status == null || r.getStatus() == status)
                .map(TabletFailure::new) // Return copies
                .collect(Collectors.toList());
    }

    @Override
    public List<TabletFailure> getCompletedMergeTablets(long beforeTime) {
        return failureRecords.values().stream()
                .filter(r -> r.getStatus() == TabletStatus.RECOVERED && r.getRecoveryTime() != null && r.getRecoveryTime() < beforeTime)
                .map(TabletFailure::new) // Return copies
                .collect(Collectors.toList());
    }

    @Override
    public void markTempTabletCleaned(long tabletId) {
        TabletFailure record = failureRecords.get(tabletId);
        if (record != null) {
            record.setCleanupStatus(CleanupStatus.DONE);
            record.setCleanupTime(System.currentTimeMillis());
        }
    }

    @Override
    public void markCleanupFailed(long tabletId, String error) {
        TabletFailure record = failureRecords.get(tabletId);
        if (record != null) {
            record.setCleanupStatus(CleanupStatus.FAILED);
            record.setCleanupError(error);
        }
    }

    @Override
    public int getPendingCleanupCount() {
        return (int) failureRecords.values().stream()
                .filter(r -> r.getCleanupStatus() == CleanupStatus.PENDING)
                .count();
    }
}
