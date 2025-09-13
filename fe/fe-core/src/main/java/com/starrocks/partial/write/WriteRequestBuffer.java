package com.starrocks.partial.write;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WriteRequestBuffer {
    private static final Logger LOG = LogManager.getLogger(WriteRequestBuffer.class);

    private final ConcurrentHashMap<Long, Queue<WriteRequest>> bufferedWrites;
    private final ScheduledExecutorService flushExecutor;

    public WriteRequestBuffer(ScheduledExecutorService flushExecutor) {
        this.bufferedWrites = new ConcurrentHashMap<>();
        this.flushExecutor = flushExecutor;
    }

    public void bufferWrite(Long tabletId, WriteRequest request) {
        bufferedWrites.computeIfAbsent(tabletId, k -> new ConcurrentLinkedQueue<>())
                     .offer(request);

        // The design mentions a timeout handler. I'll implement a simple version of it.
        // This will check and "fail" the request if it's not flushed within the timeout.
        // In a real implementation, this would involve more sophisticated error handling.
        flushExecutor.schedule(() -> {
            Queue<WriteRequest> requests = bufferedWrites.get(tabletId);
            if (requests != null && requests.contains(request)) {
                LOG.warn("Write request for tablet {} timed out in buffer.", tabletId);
                // In a real scenario, you would probably remove the request and notify the client.
                requests.remove(request);
            }
        }, 5, TimeUnit.SECONDS); // Using the 5 seconds from the design
    }

    public void flushToTempTablet(Long tabletId, TempTablet tempTablet) {
        Queue<WriteRequest> requests = bufferedWrites.remove(tabletId);
        if (requests != null) {
            requests.forEach(request -> {
                try {
                    tempTablet.write(request);
                } catch (Exception e) {
                    // In a real implementation, you would need a robust failure handling mechanism.
                    // For example, retry, or mark the write as failed and notify the client.
                    LOG.error("Failed to write buffered request to temp tablet for original tablet {}", tabletId, e);
                    handleWriteFailure(request, e);
                }
            });
        }
    }

    private void handleWriteFailure(WriteRequest request, Exception e) {
        // Placeholder for write failure logic
        LOG.error("Handling write failure for request to tablet {}", request.getTargetTabletId(), e);
    }
}
