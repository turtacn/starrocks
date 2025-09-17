package com.starrocks.partial.merge;

import com.starrocks.common.Config;
import com.starrocks.partial.TempTabletManager;
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
    private final TempTabletManager tempTabletManager;
    private MVCCDataMerger mvccDataMerger;

    public DataMerger(TabletFailureRepository failureRepo, TempTabletManager tempTabletManager) {
        this.failureRepo = failureRepo;
        this.tempTabletManager = tempTabletManager;
        this.mergeExecutor = Executors.newSingleThreadScheduledExecutor();
        this.mvccDataMerger = new MVCCDataMerger();
    }

    @VisibleForTesting
    void setMvccDataMerger(MVCCDataMerger mvccDataMerger) {
        this.mvccDataMerger = mvccDataMerger;
    }

    public void startMergeDaemon() {
        if (Config.auto_merge_on_recovery) {
            mergeExecutor.scheduleWithFixedDelay(this::scanAndMerge, 30, 60, TimeUnit.SECONDS);
        }
    }

    void scanAndMerge() {
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

    private void mergeTabletData(TabletFailure failure) {
        MergeResult result = mvccDataMerger.mergeTabletData(failure);

        if (result.getStatus() == MergeStatus.SUCCESS) {
            failure.setStatus(TabletStatus.RECOVERED);
            failure.setRecoveryTime(System.currentTimeMillis());
            failureRepo.save(failure);
            GlobalStateMgr.getCurrentState().getEditLog().logSaveTabletFailure(failure);
            cleanupTemporaryTablet(failure);
        } else {
            LOG.error("Failed to merge tablet {}: {}", failure.getTabletId(), result.getError());
        }
    }

    private void cleanupTemporaryTablet(TabletFailure failure) {
        try {
            TempTablet tempTablet = tempTabletManager.getTempTablet(failure.getTabletId())
                    .orElseThrow(() -> new RuntimeException("Temp tablet not found for original tablet " + failure.getTabletId()));

            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getDb(failure.getTableId())
                    .getTable(failure.getTableId());
            if (table != null) {
                Partition tempPartition = table.getTempPartition(tempTablet.getTempPartitionId());
                if (tempPartition != null) {
                    table.dropTempPartition(tempPartition.getName(), true);
                    LOG.info("Dropped temporary partition {} for tablet {}", tempPartition.getName(), failure.getTabletId());
                }
            }
            tempTabletManager.removeTempTablet(failure.getTabletId());
        } catch (Exception e) {
            LOG.error("Failed to cleanup temporary tablet for original tablet {}", failure.getTabletId(), e);
        }
    }
}
