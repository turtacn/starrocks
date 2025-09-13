package com.starrocks.partial.write;

import com.starrocks.qe.RowBatch;

public class WriteRequest {
    private final long tabletId;
    private final RowBatch rowBatch;

    public WriteRequest(long tabletId, RowBatch rowBatch) {
        this.tabletId = tabletId;
        this.rowBatch = rowBatch;
    }

    public long getTargetTabletId() {
        return tabletId;
    }

    public RowBatch getRowBatch() {
        return rowBatch;
    }
}
