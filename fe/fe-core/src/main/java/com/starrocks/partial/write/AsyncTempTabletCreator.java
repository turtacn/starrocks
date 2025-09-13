package com.starrocks.partial.write;

import com.starrocks.catalog.Column;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncTempTabletCreator {
    private static final Logger LOG = LogManager.getLogger(AsyncTempTabletCreator.class);

    private final ExecutorService creatorExecutor;
    private final ConcurrentHashMap<Long, CompletableFuture<TempTablet>> creationTasks;
    private final ConcurrentHashMap<Long, TempTablet> tempTabletCache;
    private final AtomicLong nextTempTabletId = new AtomicLong(90000L);

    public AsyncTempTabletCreator(ExecutorService executorService) {
        this.creatorExecutor = executorService;
        this.creationTasks = new ConcurrentHashMap<>();
        this.tempTabletCache = new ConcurrentHashMap<>();
    }

    public CompletableFuture<TempTablet> createTempTabletAsync(Long originalTabletId,
                                                               List<Column> schema) {
        return creationTasks.computeIfAbsent(originalTabletId, id ->
            CompletableFuture.supplyAsync(() -> {
                try {
                    TempTablet cachedTablet = tempTabletCache.get(id);
                    if (cachedTablet != null) {
                        return cachedTablet;
                    }
                    TempTablet tempTablet = doCreateTempTablet(id, schema);
                    tempTabletCache.put(id, tempTablet);
                    return tempTablet;
                } catch (Exception e) {
                    LOG.error("Failed to create temp tablet for {}", id, e);
                    creationTasks.remove(id);
                    throw new RuntimeException(e);
                }
            }, creatorExecutor)
        );
    }

    private TempTablet doCreateTempTablet(Long originalTabletId, List<Column> schema) throws Exception {
        LOG.info("Creating temporary tablet for original tablet {}", originalTabletId);
        // In a real implementation, this would involve selecting a healthy BE
        // and making an RPC call to create a new tablet.
        // For now, we simulate this by creating a mock tablet object.
        long backendId = 10001L; // Mock BE ID
        long tempTabletId = nextTempTabletId.getAndIncrement();
        return new MockTempTablet(tempTabletId, backendId);
    }
}
