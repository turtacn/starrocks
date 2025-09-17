package com.starrocks.partial.write;

public class RouteResult {
    private final RouteType routeType;
    private final long tabletId;

    public RouteResult(RouteType routeType, long tabletId) {
        this.routeType = routeType;
        this.tabletId = tabletId;
    }

    public RouteType getRouteType() {
        return routeType;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getTemporaryTabletId() {
        if (routeType == RouteType.TEMPORARY_STORAGE) {
            return tabletId;
        }
        return -1;
    }
}
