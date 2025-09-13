package com.starrocks.partial;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class PartialAvailabilityTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
        starRocksAssert.withTable("CREATE TABLE t1 (k1 int, v1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
        starRocksAssert.ddl("insert into t1 values(1,1), (2,2), (3,3), (4,4), (5,5), (6,6);");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        starRocksAssert.dropTable("t1");
        starRocksAssert.dropDatabase("test");
    }

    @Test
    public void testPartialAvailability() throws Exception {
        Config.partial_available_enabled = true;

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getDb("test").getTable("t1");
        List<Long> tabletIds = table.getPartitions().stream().flatMap(p -> p.getBaseIndex().getTablets().stream()).mapToLong(t -> t.getId()).boxed().collect(Collectors.toList());
        long failedTabletId = tabletIds.get(0);

        new MockUp<FailureDetector>() {
            @Mock
            public boolean isTabletFailed(long tabletId) {
                return tabletId == failedTabletId;
            }
        };

        String sql = "select * from t1";
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
        List<PlanNode> scanNodes = execPlan.getScanNodes();
        Assert.assertEquals(1, scanNodes.size());
        OlapScanNode scanNode = (OlapScanNode) scanNodes.get(0);
        Assert.assertEquals(2, scanNode.getScanTabletIds().size());
    }
}
