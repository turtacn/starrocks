package com.starrocks.partial;

import com.google.common.collect.Maps;
import com.starrocks.partial.write.TempTablet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

public class TempTabletManager {
    private static final Logger LOG = LogManager.getLogger(TempTabletManager.class);

    private final Map<Long, TempTablet> tempTabletMap = Maps.newConcurrentMap();

    public void addTempTablet(long originalTabletId, TempTablet tempTablet) {
        tempTabletMap.put(originalTabletId, tempTablet);
        LOG.info("Added temp tablet {} for original tablet {}", tempTablet, originalTabletId);
    }

    public Optional<TempTablet> getTempTablet(long originalTabletId) {
        return Optional.ofNullable(tempTabletMap.get(originalTabletId));
    }

    public void removeTempTablet(long originalTabletId) {
        tempTabletMap.remove(originalTabletId);
        LOG.info("Removed temp tablet for original tablet {}", originalTabletId);
    }
}
