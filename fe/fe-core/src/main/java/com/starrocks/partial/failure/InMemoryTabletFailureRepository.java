package com.starrocks.partial.failure;

import com.starrocks.partial.CleanupStatus;
import com.starrocks.partial.TabletStatus;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InMemoryTabletFailureRepository implements TabletFailureRepository {

    private final ConcurrentHashMap<Long, TabletFailure> tabletFailures = new ConcurrentHashMap<>();

    @Override
    public void save(TabletFailure tabletFailure) {
        tabletFailures.put(tabletFailure.getTabletId(), tabletFailure);
    }

    @Override
    public TabletFailure findByTabletId(long tabletId) {
        return tabletFailures.get(tabletId);
    }

    @Override
    public void deleteByTabletId(long tabletId) {
        tabletFailures.remove(tabletId);
    }

    @Override
    public List<TabletFailure> getTabletsByStatus(TabletStatus status) {
        return tabletFailures.values().stream()
                .filter(failure -> failure.getStatus() == status)
                .collect(Collectors.toList());
    }

    @Override
    public List<TabletFailure> getCompletedMergeTablets(long beforeTime) {
        return tabletFailures.values().stream()
                .filter(failure -> failure.getStatus() == TabletStatus.RECOVERED)
                .filter(failure -> failure.getRecoveryTime() != null && failure.getRecoveryTime() < beforeTime)
                .collect(Collectors.toList());
    }

    @Override
    public void markTempTabletCleaned(long tabletId) {
        TabletFailure failure = findByTabletId(tabletId);
        if (failure != null) {
            failure.setCleanupStatus(CleanupStatus.CLEANED);
            failure.setCleanupTime(System.currentTimeMillis());
            save(failure);
        }
    }

    @Override
    public void markCleanupFailed(long tabletId, String error) {
        TabletFailure failure = findByTabletId(tabletId);
        if (failure != null) {
            failure.setCleanupStatus(CleanupStatus.CLEANUP_FAILED);
            failure.setCleanupError(error);
            save(failure);
        }
    }

    @Override
    public int getPendingCleanupCount() {
        return (int) tabletFailures.values().stream()
                .filter(failure -> failure.getCleanupStatus() == CleanupStatus.NOT_CLEANED
                                    && failure.getStatus() == TabletStatus.RECOVERED)
                .count();
    }
}
