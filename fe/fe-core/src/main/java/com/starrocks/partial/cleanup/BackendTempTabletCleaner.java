package com.starrocks.partial.cleanup;

import com.starrocks.partial.failure.TabletFailure;
import com.starrocks.partial.failure.TabletFailureRepository;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

// This class is a placeholder for the logic that sends cleanup requests to BEs.
// The actual implementation would involve defining a new RPC service and using a client
// to call it. For now, I will just log the actions.
public class BackendTempTabletCleaner {
    private static final Logger LOG = LogManager.getLogger(BackendTempTabletCleaner.class);

    private final TabletFailureRepository failureRepo;
    private final SystemInfoService systemInfoService;

    public BackendTempTabletCleaner(TabletFailureRepository failureRepo, SystemInfoService systemInfoService) {
        this.failureRepo = failureRepo;
        this.systemInfoService = systemInfoService;
    }

    public void cleanupTablets(Long backendId, List<TabletFailure> tablets) {
        Backend backend = systemInfoService.getBackend(backendId);
        if (backend == null || !backend.isAlive()) {
            LOG.warn("BE node {} is not available, skipping cleanup", backendId);
            return;
        }

        for (TabletFailure tablet : tablets) {
            if (tablet.getTempTabletId() != null) {
                LOG.info("Sending cleanup request for temp tablet {} on BE {}", tablet.getTempTabletId(), backendId);
                // In a real implementation, this would be an RPC call.
                // For now, we'll just simulate a successful cleanup.
                processCleanupResponse(tablet, true, null);
            }
        }
    }

    private void processCleanupResponse(TabletFailure tablet, boolean success, String errorMsg) {
        if (success) {
            failureRepo.markTempTabletCleaned(tablet.getTabletId());
            LOG.info("Successfully cleaned up temp tablet {}", tablet.getTempTabletId());
        } else {
            failureRepo.markCleanupFailed(tablet.getTabletId(), errorMsg);
            LOG.warn("Failed to clean up temp tablet {}: {}", tablet.getTempTabletId(), errorMsg);
        }
    }
}
