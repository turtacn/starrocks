package com.starrocks.partial.merge;

import com.starrocks.partial.TabletStatus;
import com.starrocks.partial.failure.TabletFailure;
import com.starrocks.partial.failure.TabletFailureRepository;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DataMerger {
    private static final Logger LOG = LogManager.getLogger(DataMerger.class);

    private final ScheduledExecutorService mergeExecutor;
    private final TabletFailureRepository failureRepo;
    // The design mentions TempTabletManager, I'll assume it's a placeholder for now.
    // private final TempTabletManager tempTabletManager;

    public DataMerger(TabletFailureRepository failureRepo) {
        this.failureRepo = failureRepo;
        this.mergeExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    public void startMergeDaemon() {
        mergeExecutor.scheduleWithFixedDelay(this::scanAndMerge, 30, 60, TimeUnit.SECONDS);
    }

    private void scanAndMerge() {
        List<TabletFailure> recoveringTablets =
                failureRepo.getTabletsByStatus(TabletStatus.RECOVERING);

        for (TabletFailure failure : recoveringTablets) {
            CompletableFuture.runAsync(() -> {
                try {
                    mergeTabletData(failure);
                } catch (Exception e) {
                    LOG.error("Failed to merge tablet {}", failure.getTabletId(), e);
                }
            }, mergeExecutor);
        }
    }

    private MergeResult mergeTabletData(TabletFailure failure) {
        // This will call MVCCDataMerger
        MVCCDataMerger mvccDataMerger = new MVCCDataMerger();
        return mvccDataMerger.mergeTabletData(failure);
    }
}
