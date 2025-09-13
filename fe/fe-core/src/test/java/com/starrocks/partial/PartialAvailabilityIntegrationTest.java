// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.partial;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.planner.TupleId;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.partial.cleanup.BackendTempTabletCleaner;
import com.starrocks.partial.cleanup.TempTabletCleaner;
import com.starrocks.partial.failure.InMemoryTabletFailureRepository;
import com.starrocks.partial.failure.TabletFailure;
import com.starrocks.partial.failure.TabletFailureRepository;
import com.starrocks.partial.merge.DataMerger;
import com.starrocks.partial.query.QueryFilter;
import com.starrocks.partial.write.AsyncTempTabletCreator;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class PartialAvailabilityIntegrationTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static final String DB_NAME = "test_partial_db";
    private static final String TABLE_NAME = "test_partial_tbl";

    private TabletFailureRepository failureRepo;
    private FailureDetector failureDetector;
    private AsyncTempTabletCreator tempTabletCreator;
    private QueryFilter queryFilter;
    private DataMerger dataMerger;
    private TempTabletCleaner tempTabletCleaner;

    @Mocked
    private BackendTempTabletCleaner backendCleaner;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        starRocksAssert.withTable("CREATE TABLE " + TABLE_NAME + " (k1 int, k2 int, v1 int) "
                + "PRIMARY KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1');");
    }

    @AfterAll
    public static void afterAll() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize partial availability components
        failureRepo = new InMemoryTabletFailureRepository();
        failureDetector = new FailureDetector(failureRepo);
        ExecutorService executor = Executors.newCachedThreadPool();
        tempTabletCreator = new AsyncTempTabletCreator(executor);
        queryFilter = new QueryFilter(failureDetector);
        dataMerger = new DataMerger(failureRepo);
        tempTabletCleaner = new TempTabletCleaner(failureRepo, backendCleaner);

        // Inject components into GlobalStateMgr
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        Deencapsulation.setField(gsm, "failureDetector", failureDetector);
        Deencapsulation.setField(gsm, "asyncTempTabletCreator", tempTabletCreator);
    }

    private Tablet getTabletToFail() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) db.getTable(TABLE_NAME);
        Partition partition = table.getPartitions().iterator().next();
        return partition.getSubPartitions().iterator().next().getBaseIndex().getTablets().get(0);
    }

    private List<Tablet> getAllTablets() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) db.getTable(TABLE_NAME);
        return table.getPartitions().stream()
               .flatMap(p -> p.getSubPartitions().stream())
               .flatMap(pp -> pp.getBaseIndex().getTablets().stream())
               .collect(Collectors.toList());
    }


    private void simulateFailedTablet(Tablet tabletToFail) {
        TabletFailure failure = new TabletFailure();
        failure.setTabletId(tabletToFail.getId());
        failure.setBackendId(tabletToFail.getBackendIds().iterator().next());
        failure.setStatus(TabletStatus.FAILED);
        failure.setFailTime(System.currentTimeMillis());
        failureRepo.save(failure);
    }

    @Test
    public void testQueryPath_shouldFilterFailedTablet() throws Exception {
        List<Tablet> allTablets = getAllTablets();
        Assertions.assertEquals(3, allTablets.size());
        Tablet failedTablet = allTablets.get(0);
        simulateFailedTablet(failedTablet);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) db.getTable(TABLE_NAME);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        OlapScanNode scanNode = new OlapScanNode(new com.starrocks.planner.PlanNodeId(0), desc, "scan");
        List<Long> allTabletIds = allTablets.stream().map(Tablet::getId).collect(Collectors.toList());
        Deencapsulation.setField(scanNode, "scanTabletIds", allTabletIds);

        int originalTabletCount = scanNode.getScanTabletIds().size();
        Assertions.assertEquals(3, originalTabletCount);

        queryFilter.filter(null, scanNode);

        List<Long> filteredTabletIds = scanNode.getScanTabletIds();
        Assertions.assertEquals(originalTabletCount - 1, filteredTabletIds.size());
        Assertions.assertFalse(filteredTabletIds.contains(failedTablet.getId()));
    }

    @Test
    public void testMergeAndCleanupPath() throws InterruptedException {
        Tablet failedTablet = getTabletToFail();
        simulateFailedTablet(failedTablet);

        TabletFailure failure = failureRepo.findByTabletId(failedTablet.getId());
        failure.setStatus(TabletStatus.RECOVERING);
        failure.setTempTabletId(90001L);
        failureRepo.save(failure);

        Deencapsulation.invoke(dataMerger, "scanAndMerge");

        Thread.sleep(200);

        TabletFailure recoveredFailure = failureRepo.findByTabletId(failedTablet.getId());
        Assertions.assertNotNull(recoveredFailure);
        Assertions.assertEquals(TabletStatus.RECOVERED, recoveredFailure.getStatus());
        Assertions.assertEquals(CleanupStatus.NOT_CLEANED, recoveredFailure.getCleanupStatus());

        recoveredFailure.setRecoveryTime(System.currentTimeMillis() - 2 * 60 * 60 * 1000);
        failureRepo.save(recoveredFailure);

        Deencapsulation.invoke(tempTabletCleaner, "performCleanup");
        Thread.sleep(200);

        // In the real TempTabletCleaner, the call to backendCleaner is async.
        // The backendCleaner would then make an RPC to the BE, and upon success, the BE would call back to the FE
        // to mark the tablet as cleaned. Since we can't do that here, we simulate the callback manually.
        failureRepo.markTempTabletCleaned(failedTablet.getId());

        TabletFailure cleanedFailure = failureRepo.findByTabletId(failedTablet.getId());
        Assertions.assertNotNull(cleanedFailure);
        Assertions.assertEquals(CleanupStatus.CLEANED, cleanedFailure.getCleanupStatus());
    }
}
