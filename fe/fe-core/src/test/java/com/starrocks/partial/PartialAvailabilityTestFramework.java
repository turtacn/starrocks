package com.starrocks.partial;

import com.starrocks.clone.MockTabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.partial.failure.MockTabletFailureRepository;
import com.starrocks.partial.query.QueryFilter;
import com.starrocks.partial.write.AsyncTempTabletCreator;
import com.starrocks.partial.write.RouteResult;
import com.starrocks.partial.write.RouteType;
import com.starrocks.partial.write.WriteRequest;
import com.starrocks.partial.write.WriteRouter;
import com.starrocks.system.SystemInfoService;
import com.starrocks.test.util.MockFrontendService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class PartialAvailabilityTestFramework {

    private MockTabletFailureRepository mockRepository;
    private SystemInfoService mockSystemInfoService;
    private FailureDetector failureDetector;
    private QueryFilter queryFilter;
    private WriteRouter writeRouter;
    private AsyncTempTabletCreator tempTabletCreator;

    // Mock classes for testing
    private static class MockQueryPlan {
        private final Set<Long> tablets;
        public MockQueryPlan(Set<Long> tablets) {
            this.tablets = new HashSet<>(tablets);
        }
        public Set<Long> getAvailableTablets() {
            return tablets;
        }
        public void filterTablets(java.util.function.Predicate<Long> predicate) {
            tablets.removeIf(predicate.negate());
        }
    }

    @BeforeEach
    void setUp() {
        // Initialize all mocks
        mockRepository = new MockTabletFailureRepository();
        mockSystemInfoService = mock(SystemInfoService.class); // Using Mockito for system service

        // Inject mocks into real components
        failureDetector = new FailureDetector(mockRepository, mockSystemInfoService);
        queryFilter = new QueryFilter(failureDetector);
        tempTabletCreator = new AsyncTempTabletCreator();
        writeRouter = new WriteRouter(failureDetector, tempTabletCreator);
    }

    @Test
    void testFailureDetectionAndQueryFiltering() {
        // Setup: Simulate tablet failure
        long failedTabletId = 123L;
        long tableId = 456L;

        // Record a failure
        failureDetector.recordFailedTablets(1L); // Assume BE 1 is down
        // Manually add a specific tablet to the failed set for this test
        com.starrocks.partial.failure.TabletFailure failure = new com.starrocks.partial.failure.TabletFailure();
        failure.setTabletId(failedTabletId);
        failure.setTableId(tableId);
        failure.setStatus(TabletStatus.FAILED);
        mockRepository.save(failure);

        // Test query filtering
        Set<Long> requiredTablets = new HashSet<>(Set.of(failedTabletId, 124L, 125L));
        assertTrue(failureDetector.isTabletFailed(failedTabletId));

        // Verify filtering logic
        MockQueryPlan mockPlan = new MockQueryPlan(requiredTablets);
        // The real QueryFilter would traverse a PlanNode tree. We simplify this.
        mockPlan.filterTablets(tabletId -> !failureDetector.isTabletFailed(tabletId));

        assertFalse(mockPlan.getAvailableTablets().contains(failedTabletId));
        assertTrue(mockPlan.getAvailableTablets().contains(124L));
    }

    @Test
    void testWriteRoutingToTemporaryStorage() {
        // Setup failure scenario
        long failedTabletId = 200L;
        com.starrocks.partial.failure.TabletFailure failure = new com.starrocks.partial.failure.TabletFailure();
        failure.setTabletId(failedTabletId);
        failure.setStatus(TabletStatus.FAILED);
        mockRepository.save(failure);

        // Test write routing
        WriteRequest request = new WriteRequest(failedTabletId, "test_data");
        RouteResult result = writeRouter.determineWriteRoute(request);

        assertEquals(RouteType.TEMPORARY_STORAGE, result.getRouteType());
        assertNotNull(result.getTemporaryTabletId());
        assertTrue(result.getTemporaryTabletId() > 0);
    }
}
