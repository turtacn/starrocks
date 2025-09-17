package com.starrocks.partial.failure;

import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.partial.TabletFailureMgr;
import com.starrocks.partial.TabletStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FailureDetector {
    private static final Logger LOG = LogManager.getLogger(FailureDetector.class);

    private final TabletFailureRepository tabletFailureRepository;

    public FailureDetector(TabletFailureRepository tabletFailureRepository) {
        this.tabletFailureRepository = tabletFailureRepository;
    }

    public void detectNodeFailures() {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<Long> backendIds = systemInfoService.getBackendIds(false);
        for (long backendId : backendIds) {
            Backend backend = systemInfoService.getBackend(backendId);
            if (backend == null || !backend.isAlive()) {
                recordFailedTablets(backendId);
            } else {
                handleNodeRecovery(backendId);
            }
        }
    }

    public void recordFailedTablets(long backendId) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        List<Long> tablets = invertedIndex.getTabletIdsByBackendId(backendId);
        for (long tabletId : tablets) {
            TabletFailure existingFailure = tabletFailureRepository.findByTabletId(tabletId);
            if (existingFailure == null) {
                long tableId = invertedIndex.getTableId(tabletId);
                long partitionId = invertedIndex.getPartitionId(tabletId);
                TabletFailure failure = new TabletFailure(tabletId, backendId, partitionId, tableId,
                        System.currentTimeMillis(), TabletStatus.FAILED);
                tabletFailureRepository.save(failure);
                GlobalStateMgr.getCurrentState().getEditLog().logSaveTabletFailure(failure);
                LOG.info("Recorded failed tablet {} on backend {}", tabletId, backendId);
            }
        }
    }

    public void handleNodeRecovery(long backendId) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        List<Long> tablets = invertedIndex.getTabletIdsByBackendId(backendId);
        for (long tabletId : tablets) {
            TabletFailure existingFailure = tabletFailureRepository.findByTabletId(tabletId);
            if (existingFailure != null && existingFailure.getStatus() == TabletStatus.FAILED) {
                existingFailure.setStatus(TabletStatus.RECOVERING);
                existingFailure.setRecoveryTime(System.currentTimeMillis());
                tabletFailureRepository.save(existingFailure);
                GlobalStateMgr.getCurrentState().getEditLog().logSaveTabletFailure(existingFailure);
                LOG.info("Tablet {} on backend {} is recovering", tabletId, backendId);
            }
        }
    }

    public Set<Long> getFailedTablets(long tableId) {
        return tabletFailureRepository.getTabletsByStatus(TabletStatus.FAILED)
                .stream()
                .filter(failure -> failure.getTableId() == tableId)
                .map(TabletFailure::getTabletId)
                .collect(Collectors.toSet());
    }

    public boolean isTabletFailed(long tabletId) {
        TabletFailure failure = tabletFailureRepository.findByTabletId(tabletId);
        return failure != null && failure.getStatus() == TabletStatus.FAILED;
    }
}
