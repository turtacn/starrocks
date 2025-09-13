package com.starrocks.partial.write;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MockTempTablet implements TempTablet {
    private static final Logger LOG = LogManager.getLogger(MockTempTablet.class);

    private final long tempTabletId;
    private final long backendId;

    public MockTempTablet(long tempTabletId, long backendId) {
        this.tempTabletId = tempTabletId;
        this.backendId = backendId;
    }

    @Override
    public void write(WriteRequest request) throws Exception {
        LOG.info("Writing to mock temp tablet {} on backend {}", tempTabletId, backendId);
    }

    public long getTempTabletId() {
        return tempTabletId;
    }

    public long getBackendId() {
        return backendId;
    }
}
