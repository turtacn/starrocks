package com.starrocks.partial.write;

public interface TempTablet {
    long getTempTabletId();

    long getBackendId();

    void write(WriteRequest request) throws Exception;
}
