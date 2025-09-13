package com.starrocks.partial;

import com.starrocks.partial.failure.TabletFailure;
import com.starrocks.partial.failure.TabletFailureRepository;

public class FailureDetector {

    private final TabletFailureRepository tabletFailureRepository;

    public FailureDetector(TabletFailureRepository tabletFailureRepository) {
        this.tabletFailureRepository = tabletFailureRepository;
    }

    public boolean isTabletFailed(long tabletId) {
        if (tabletFailureRepository == null) {
            return false;
        }
        TabletFailure failure = tabletFailureRepository.findByTabletId(tabletId);
        if (failure == null) {
            return false;
        }
        return failure.getStatus() == TabletStatus.FAILED || failure.getStatus() == TabletStatus.RECOVERING;
    }

    public TabletFailureRepository getTabletFailureRepository() {
        return tabletFailureRepository;
    }
}
