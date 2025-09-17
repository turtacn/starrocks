package com.starrocks.partial.write;

import com.starrocks.partial.FailureDetector;

public class WriteRouter {
    private final FailureDetector failureDetector;
    private final AsyncTempTabletCreator tempTabletCreator;

    public WriteRouter(FailureDetector failureDetector, AsyncTempTabletCreator tempTabletCreator) {
        this.failureDetector = failureDetector;
        this.tempTabletCreator = tempTabletCreator;
    }

    public RouteResult determineWriteRoute(WriteRequest request) {
        if (failureDetector.isTabletFailed(request.getTabletId())) {
            // Tablet is failed, route to temporary storage.
            // This would involve creating a temporary tablet.
            long tempTabletId = tempTabletCreator.createTempTablet(request.getTabletId());
            return new RouteResult(RouteType.TEMPORARY_STORAGE, tempTabletId);
        } else {
            // Tablet is healthy, route to original storage.
            return new RouteResult(RouteType.ORIGINAL_STORAGE, request.getTabletId());
        }
    }
}
