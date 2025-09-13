package com.starrocks.partial.cleanup;

import com.starrocks.metric.GaugeMetric;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.partial.failure.TabletFailureRepository;

public class TempTabletCleanupMonitor {
    private static final GaugeMetric<Long> PENDING_CLEANUP_COUNT =
            new GaugeMetric<Long>("temp_tablet_pending_cleanup", Metric.MetricUnit.NOUNIT, "Number of temp tablets pending cleanup") {
                @Override
                public Long getValue() {
                    // This is a placeholder. The actual value should be updated periodically.
                    return 0L;
                }
            };

    private static final LongCounterMetric CLEANUP_SUCCESS_COUNT =
            new LongCounterMetric("temp_tablet_cleanup_success", Metric.MetricUnit.REQUESTS, "Number of successfully cleaned temp tablets");

    private static final LongCounterMetric CLEANUP_FAILURE_COUNT =
            new LongCounterMetric("temp_tablet_cleanup_failure", Metric.MetricUnit.REQUESTS, "Number of failed temp tablet cleanups");

    private final TabletFailureRepository failureRepo;

    public TempTabletCleanupMonitor(TabletFailureRepository failureRepo) {
        this.failureRepo = failureRepo;
    }

    public void register() {
        MetricRepo.addMetric(PENDING_CLEANUP_COUNT);
        MetricRepo.addMetric(CLEANUP_SUCCESS_COUNT);
        MetricRepo.addMetric(CLEANUP_FAILURE_COUNT);
    }

    public void updateMetrics() {
        // In a real implementation, this would be called to update the gauge.
        // For now, this is a placeholder.
    }
}
