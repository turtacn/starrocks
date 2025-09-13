// fe/fe-core/src/test/java/com/starrocks/planner/OlapScanNodeTest.java
package com.starrocks.planner;

import com.starrocks.planner.TupleDescriptor;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.jmockit.Expectations;
import com.starrocks.sql.plan.ExecPlan;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class OlapScanNodeTest {
    @Test
    public void testFilterTablets(@Mocked OlapTable olapTable,
                                 @Mocked TupleDescriptor desc,
                                 @Mocked ExecPlan execPlan) {
        new Expectations() {
            {
                desc.getTable();
                result = olapTable;
            }
        };

        OlapScanNode olapScanNode = new OlapScanNode(new PlanNodeId(0), desc, "olapScanNode");
        List<Long> tabletIds = new ArrayList<>();
        tabletIds.add(1L);
        tabletIds.add(2L);
        tabletIds.add(3L);
        olapScanNode.scanTabletIds = tabletIds;

        olapScanNode.filterTablets(tabletId -> tabletId > 1);

        Assert.assertEquals(2, olapScanNode.getScanTabletIds().size());
        Assert.assertTrue(olapScanNode.getScanTabletIds().contains(2L));
        Assert.assertTrue(olapScanNode.getScanTabletIds().contains(3L));
    }
}
