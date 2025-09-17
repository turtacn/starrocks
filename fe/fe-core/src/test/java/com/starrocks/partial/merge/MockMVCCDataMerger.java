package com.starrocks.partial.merge;

import com.starrocks.partial.failure.TabletFailure;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockMVCCDataMerger extends MVCCDataMerger {
    private Map<Long, Boolean> mergeResults = new HashMap<>();
    private Map<Long, List<String>> mergeOperations = new HashMap<>();

    @Override
    public MergeResult mergeTabletData(TabletFailure failure) {
        long originalTabletId = failure.getTabletId();
        Long tempTabletId = failure.getTempTabletId();

        // Mock successful merge
        String operation = (tempTabletId != null)
            ? "MERGE_TEMP_DATA: " + tempTabletId + " -> " + originalTabletId
            : "MERGE_TEMP_DATA: unknown_temp_tablet -> " + originalTabletId;
        mergeOperations.computeIfAbsent(originalTabletId, k -> new ArrayList<>()).add(operation);
        mergeResults.put(originalTabletId, true);

        MergeResult result = new MergeResult();
        result.setStatus(MergeStatus.SUCCESS);
        return result;
    }

    // Verification methods for testing
    public boolean wasMergeCalled(long tabletId) {
        return mergeResults.containsKey(tabletId);
    }

    public List<String> getMergeOperations(long tabletId) {
        return mergeOperations.getOrDefault(tabletId, Collections.emptyList());
    }
}
