package com.starrocks.partial.cleanup;

import com.starrocks.common.Config;
import com.starrocks.metric.GaugeMetric;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.partial.failure.TabletFailure;
import com.starrocks.partial.failure.TabletFailureRepository;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TempTabletCleanupMonitor {
    private final TabletFailureRepository failureRepo;
    private final TempTabletCleaner cleaner;
    private final ScheduledExecutorService cleanupExecutor;

    public TempTabletCleanupMonitor(TabletFailureRepository failureRepo, TempTabletCleaner cleaner) {
        this.failureRepo = failureRepo;
        this.cleaner = cleaner;
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        cleanupExecutor.scheduleWithFixedDelay(this::cleanupExpiredTempTablets, 1, 1, TimeUnit.HOURS);
    }

    private void cleanupExpiredTempTablets() {
        long retentionMillis = TimeUnit.HOURS.toMillis(Config.temp_storage_max_retention);
        long cutoffTime = System.currentTimeMillis() - retentionMillis;
        List<TabletFailure> expiredFailures = failureRepo.getCompletedMergeTablets(cutoffTime);

        for (TabletFailure failure : expiredFailures) {
            try {
                cleaner.cleanup(failure);
            } catch (Exception e) {
                // Log and continue
            }
        }
    }
}
