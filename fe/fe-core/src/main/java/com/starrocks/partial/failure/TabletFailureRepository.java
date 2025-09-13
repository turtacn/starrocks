package com.starrocks.partial.failure;

import com.starrocks.partial.TabletStatus;

import java.util.List;

public interface TabletFailureRepository {

    void save(TabletFailure tabletFailure);

    TabletFailure findByTabletId(long tabletId);

    void deleteByTabletId(long tabletId);

    List<TabletFailure> getTabletsByStatus(TabletStatus status);

    List<TabletFailure> getCompletedMergeTablets(long beforeTime);

    void markTempTabletCleaned(long tabletId);

    void markCleanupFailed(long tabletId, String error);

    int getPendingCleanupCount();
}
