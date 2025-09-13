// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.partial.merge;

import com.starrocks.common.DdlException;
import com.starrocks.partial.CleanupStatus;
import com.starrocks.partial.TabletStatus;
import com.starrocks.partial.failure.TabletFailure;
import com.starrocks.partial.failure.TabletFailureRepository;
import com.starrocks.qe.RowBatch;
import com.starrocks.server.GlobalStateMgr;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MVCCDataMerger {
    private static final Logger LOG = LogManager.getLogger(MVCCDataMerger.class);

    private TabletFailureRepository getFailureRepo() {
        // In a real implementation, this would be injected. For testing, we get it from the GlobalStateMgr.
        // This assumes the manager and its components have been initialized for the test.
        return GlobalStateMgr.getCurrentState().getFailureDetector().getTabletFailureRepository();
    }

    public MergeResult mergeTabletData(TabletFailure failure) {
        Long originalTabletId = failure.getTabletId();
        Long tempTabletId = failure.getTempTabletId();

        try {
            LOG.info("Starting merge for original tablet {} and temp tablet {}", originalTabletId, tempTabletId);

            List<Long> originalVersions = getTabletVersions(originalTabletId);
            List<Long> tempVersions = getTabletVersions(tempTabletId);

            MergeStrategy strategy = calculateMergeStrategy(originalVersions, tempVersions);

            return executeMerge(originalTabletId, tempTabletId, strategy, failure);

        } catch (Exception e) {
            LOG.error("Failed to merge tablet data: originalTablet={}, tempTablet={}",
                     originalTabletId, tempTabletId, e);
            throw new RuntimeException("Data merge failed", e);
        }
    }

    // Mock implementation
    private List<Long> getTabletVersions(Long tabletId) {
        LOG.debug("Getting versions for tablet {}", tabletId);
        List<Long> versions = new ArrayList<>();
        if (tabletId < 90000) { // Original Tablet
            versions.add(2L);
            versions.add(3L);
        } else { // Temp Tablet
            versions.add(4L);
            versions.add(5L);
        }
        return versions;
    }

    private MergeStrategy calculateMergeStrategy(List<Long> originalVersions, List<Long> tempVersions) {
        MergeStrategy strategy = new MergeStrategy();

        // Versions on the original tablet are considered the base
        strategy.setBaseVersions(originalVersions);
        // Versions on the temp tablet are the incremental changes
        strategy.setIncrementalVersions(tempVersions);
        strategy.setMergeType(determineMergeType(originalVersions, tempVersions));

        LOG.info("Calculated merge strategy for tablets. Type: {}, Base versions: {}, Incremental versions: {}",
                strategy.getMergeType(), originalVersions.size(), tempVersions.size());

        return strategy;
    }

    private MergeType determineMergeType(List<Long> originalVersions, List<Long> tempVersions) {
        // This could be more sophisticated, but for now, incremental is the main goal.
        return MergeType.INCREMENTAL;
    }

    private MergeResult executeMerge(Long originalTabletId, Long tempTabletId,
                                   MergeStrategy strategy, TabletFailure failure) {
        switch (strategy.getMergeType()) {
            case INCREMENTAL:
                return executeIncrementalMerge(originalTabletId, tempTabletId, strategy, failure);
            case FULL:
            case CONFLICT_RESOLUTION:
            default:
                throw new IllegalArgumentException("Unsupported merge type: " + strategy.getMergeType());
        }
    }

    private MergeResult executeIncrementalMerge(Long originalTabletId, Long tempTabletId,
                                          MergeStrategy strategy, TabletFailure failure) {
        MergeResult result = new MergeResult();
        TabletFailureRepository failureRepo = getFailureRepo();
        try {
            LOG.info("Executing incremental merge for original tablet {}", originalTabletId);
            for (Long version : strategy.getIncrementalVersions()) {
                // Simulate reading data from temp tablet
                RowBatch incrementalData = readVersionData(tempTabletId, version);
                // Simulate applying data to original tablet
                applyIncrementalData(originalTabletId, incrementalData, version);
                result.addMergedVersion(version);
            }

            boolean validationPassed = validateMergeResult(originalTabletId, tempTabletId, result);

            if (validationPassed) {
                result.setStatus(MergeStatus.SUCCESS);
                LOG.info("Merge successful for tablet {}", originalTabletId);
                // Mark the original tablet as recovered
                failure.setStatus(TabletStatus.RECOVERED);
                failure.setRecoveryTime(System.currentTimeMillis());
                failureRepo.save(failure);
                // Trigger cleanup for the temporary tablet
                cleanupTempTablet(failure);
            } else {
                result.setStatus(MergeStatus.VALIDATION_FAILED);
                LOG.error("Merge validation failed for tablet {}", originalTabletId);
                failure.setStatus(TabletStatus.FAILED); // Revert to failed state
                failureRepo.save(failure);
            }

        } catch (Exception e) {
            result.setStatus(MergeStatus.FAILED);
            result.setError(e.getMessage());
            LOG.error("Incremental merge failed for original tablet {}", originalTabletId, e);
            failure.setStatus(TabletStatus.FAILED); // Revert to failed state
            failureRepo.save(failure);
        }
        return result;
    }

    // Mock implementation
    private RowBatch readVersionData(Long tabletId, Long version) {
        LOG.debug("Simulating read of data for version {} from tablet {}", version, tabletId);
        // In a real implementation, this would involve an RPC call to the BE.
        return new RowBatch(); // Returning empty batch as a placeholder.
    }

    // Mock implementation
    private void applyIncrementalData(Long tabletId, RowBatch data, Long version) throws DdlException {
        LOG.info("Simulating application of incremental data for version {} to tablet {}", version, tabletId);
        // In a real implementation, this would use the DeltaWriter on the BE.
    }

    // Mock implementation
    private boolean validateMergeResult(Long originalTabletId, Long tempTabletId, MergeResult result) {
        LOG.info("Simulating validation of merge result for original tablet {}", originalTabletId);
        // In a real implementation, this would involve checking row counts or checksums.
        return true;
    }

    private void cleanupTempTablet(TabletFailure failure) {
        LOG.info("Marking temporary tablet {} for cleanup", failure.getTempTabletId());
        // This doesn't delete immediately. It just marks it for the TempTabletCleaner daemon.
        // The cleaner daemon will handle the physical deletion.
        failure.setCleanupStatus(CleanupStatus.NOT_CLEANED);
        getFailureRepo().save(failure);
    }
}
