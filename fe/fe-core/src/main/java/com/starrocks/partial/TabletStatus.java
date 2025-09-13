package com.starrocks.partial;

public enum TabletStatus {
    FAILED(0),
    RECOVERING(1),
    RECOVERED(2);

    private final int value;

    TabletStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
