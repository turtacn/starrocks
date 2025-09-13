package com.starrocks.partial;

import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;

public class PartialAvailableMetrics {
    public static final LongCounterMetric TEMP_TABLET_CREATION_COUNT =
            new LongCounterMetric("temp_tablet_creation_total", Metric.MetricUnit.REQUESTS, "Total number of temporary tablets created");
    public static final LongCounterMetric BUFFERED_WRITE_COUNT =
            new LongCounterMetric("buffered_write_total", Metric.MetricUnit.REQUESTS, "Total number of buffered writes");

    public static void register() {
        MetricRepo.addMetric(TEMP_TABLET_CREATION_COUNT);
        MetricRepo.addMetric(BUFFERED_WRITE_COUNT);
    }
}
