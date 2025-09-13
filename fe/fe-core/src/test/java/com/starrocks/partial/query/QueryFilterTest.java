package com.starrocks.partial.query;

import com.starrocks.partial.FailureDetector;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNode;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Test;

import java.util.function.Predicate;

public class QueryFilterTest {

    @Test
    public void testFilter(@Mocked FailureDetector failureDetector,
                           @Mocked OlapScanNode olapScanNode,
                           @Mocked PlanNode planNode) {
        new Expectations() {
            {
                failureDetector.isTabletFailed(1L);
                result = true;
                failureDetector.isTabletFailed(2L);
                result = false;
            }
        };

        QueryFilter queryFilter = new QueryFilter(failureDetector);
        queryFilter.filter(null, olapScanNode);

        new Verifications() {
            {
                olapScanNode.filterTablets((Predicate<Long>) any);
                times = 1;
            }
        };
    }
}
