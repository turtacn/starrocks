package com.starrocks.partial;

import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.partial.failure.TabletFailure;
import com.starrocks.partial.failure.TabletFailureRepository;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class FailureDetector {
    private static final Logger LOG = LogManager.getLogger(FailureDetector.class);
    private final TabletFailureRepository tabletFailureRepository;
    private final SystemInfoService systemInfoService;

    public FailureDetector(TabletFailureRepository tabletFailureRepository, SystemInfoService systemInfoService) {
        this.tabletFailureRepository = tabletFailureRepository;
        this.systemInfoService = systemInfoService;
    }

    public void detectNodeFailures() {
        Collection<Backend> backends = systemInfoService.getBackends();
        if (backends == null) return;

        for (Backend backend : backends) {
            if (!backend.isAlive()) {
                recordFailedTablets(backend.getId());
            }
        }
    }

    public void recordFailedTablets(long backendId) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        Set<Long> tabletIds = invertedIndex.getTabletIdsByBackendId(backendId);

        for (Long tabletId : tabletIds) {
            TabletFailure existingFailure = tabletFailureRepository.findByTabletId(tabletId);
            if (existingFailure == null) {
                // In a real scenario, we'd look up partition and table IDs from metadata
                TabletFailure newFailure = new TabletFailure();
                newFailure.setTabletId(tabletId);
                newFailure.setBackendId(backendId);
                newFailure.setFailTime(System.currentTimeMillis());
                newFailure.setStatus(TabletStatus.FAILED);
                tabletFailureRepository.save(newFailure);
                // In a real implementation, we'd need to handle edit logging more carefully
                // GlobalStateMgr.getCurrentState().getEditLog().logSaveTabletFailure(newFailure);
                LOG.info("Recorded new failure for tablet {} on backend {}", tabletId, backendId);
            }
        }
    }

    public boolean isTabletFailed(long tabletId) {
        if (tabletFailureRepository == null) {
            return false;
        }
        TabletFailure failure = tabletFailureRepository.findByTabletId(tabletId);
        if (failure == null) {
            return false;
        }
        return failure.getStatus() == TabletStatus.FAILED || failure.getStatus() == TabletStatus.RECOVERING;
    }

    public Set<Long> getFailedTabletsByTable(long tableId) {
        // This is inefficient. A real implementation would need a better query.
        return tabletFailureRepository.getTabletsByStatus(TabletStatus.FAILED)
                .stream()
                .filter(f -> f.getTableId() == tableId)
                .map(TabletFailure::getTabletId)
                .collect(Collectors.toSet());
    }

    public TabletFailureRepository getTabletFailureRepository() {
        return tabletFailureRepository;
    }
}
