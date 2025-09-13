package com.starrocks.partial;

public enum CleanupStatus {
    NOT_CLEANED(0),
    CLEANING(1),
    CLEANED(2),
    CLEANUP_FAILED(3);

    private final int value;

    CleanupStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
