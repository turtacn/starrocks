package com.starrocks.partial.write;

public interface TempTablet {
    void write(WriteRequest request) throws Exception;
}
