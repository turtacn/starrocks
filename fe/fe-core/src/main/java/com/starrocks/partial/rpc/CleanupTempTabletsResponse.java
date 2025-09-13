package com.starrocks.partial.rpc;

import java.util.HashMap;
import java.util.Map;

public class CleanupTempTabletsResponse {
    private Map<Long, String> failedTablets = new HashMap<>();

    public void markSuccess(long tabletId) {
        // No-op, success is the default
    }

    public void markFailed(long tabletId, String error) {
        failedTablets.put(tabletId, error);
    }

    public boolean isSuccess(long tabletId) {
        return !failedTablets.containsKey(tabletId);
    }

    public String getError(long tabletId) {
        return failedTablets.get(tabletId);
    }
}
