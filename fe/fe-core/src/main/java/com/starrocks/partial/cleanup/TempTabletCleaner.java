package com.starrocks.partial.cleanup;

import com.starrocks.partial.failure.TabletFailure;
import com.starrocks.partial.failure.TabletFailureRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TempTabletCleaner {
    private static final Logger LOG = LogManager.getLogger(TempTabletCleaner.class);

    private final ScheduledExecutorService cleanupExecutor;
    private final TabletFailureRepository failureRepo;
    private final BackendTempTabletCleaner backendCleaner;

    public TempTabletCleaner(TabletFailureRepository failureRepo, BackendTempTabletCleaner backendCleaner) {
        this.failureRepo = failureRepo;
        this.backendCleaner = backendCleaner;
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    public void startCleanupDaemon() {
        // Every hour
        cleanupExecutor.scheduleWithFixedDelay(this::performCleanup, 60, 60, TimeUnit.MINUTES);
    }

    private void performCleanup() {
        try {
            // Get tablets that have completed merge more than 1 hour ago
            List<TabletFailure> completedMerges =
                    failureRepo.getCompletedMergeTablets(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

            Map<Long, List<TabletFailure>> tabletsByBE = completedMerges.stream()
                    .collect(Collectors.groupingBy(TabletFailure::getBackendId));

            List<CompletableFuture<Void>> cleanupTasks = new ArrayList<>();

            for (Map.Entry<Long, List<TabletFailure>> entry : tabletsByBE.entrySet()) {
                Long backendId = entry.getKey();
                List<TabletFailure> tablets = entry.getValue();

                CompletableFuture<Void> task = CompletableFuture.runAsync(() ->
                    backendCleaner.cleanupTablets(backendId, tablets), cleanupExecutor);
                cleanupTasks.add(task);
            }

            CompletableFuture.allOf(cleanupTasks.toArray(new CompletableFuture[0]))
                            .get(30, TimeUnit.MINUTES);

        } catch (Exception e) {
            LOG.error("Failed to perform temporary tablet cleanup", e);
        }
    }
}
