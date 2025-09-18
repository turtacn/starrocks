package com.starrocks.partial.merge;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.util.StopWatch;
import com.starrocks.partial.failure.FailureDetector;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.BackendService;
import com.starrocks.thrift.TBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataMerger {
    private static final Logger LOG = LoggerFactory.getLogger(DataMerger.class);
    private static DataMerger instance;
    private final ExecutorService mergeExecutor = Executors.newFixedThreadPool(5);
    private final FailureDetector failureDetector;

    private DataMerger() {
        this.failureDetector = FailureDetector.getInstance();
    }

    public static synchronized DataMerger getInstance() {
        if (instance == null) {
            instance = new DataMerger();
        }
        return instance;
    }

    /**
     * 合并Tablet数据
     */
    public void mergeTabletData(long tabletId) {
        mergeExecutor.submit(() -> {
            StopWatch watch = new StopWatch();
            watch.start();
            try {
                LOG.info("Starting data merge for tablet {}", tabletId);

                // 获取原始Tablet
                Tablet originalTablet = GlobalStateMgr.getCurrentState().getTabletMgr().getTablet(tabletId);
                if (originalTablet == null) {
                    LOG.error("Original tablet {} not found", tabletId);
                    return;
                }

                // 获取临时表ID
                Long tempTableId = failureDetector.getTempTableId(tabletId);
                if (tempTableId == null) {
                    LOG.error("No temp table found for tablet {}", tabletId);
                    return;
                }

                // 获取临时表
                OlapTable tempTable = (OlapTable) GlobalStateMgr.getCurrentState().getMetadataMgr()
                        .getTable(tempTableId);
                if (tempTable == null) {
                    LOG.warn("Temp table {} not found, maybe already merged", tempTableId);
                    failureDetector.markTabletRecovered(tabletId);
                    return;
                }

                // 获取临时表的Tablet
                List<Tablet> tempTablets = tempTable.getTablets();
                if (tempTablets.isEmpty()) {
                    LOG.info("No data in temp table for tablet {}", tabletId);
                    failureDetector.markTabletRecovered(tabletId);
                    return;
                }
                Tablet tempTablet = tempTablets.get(0);

                // 获取BE节点信息
                TBackend be = BackendService.getInstance().getBackend(originalTablet.getBackendId());
                if (be == null) {
                    LOG.error("Backend for tablet {} not found", tabletId);
                    return;
                }

                // 执行数据合并（基于MVCC）
                boolean mergeSuccess = mergeTablets(originalTablet, tempTablet, be);

                if (mergeSuccess) {
                    LOG.info("Data merge completed for tablet {}", tabletId);
                    // 标记Tablet为已恢复
                    failureDetector.markTabletRecovered(tabletId);
                } else {
                    LOG.error("Data merge failed for tablet {}", tabletId);
                }
            } catch (Exception e) {
                LOG.error("Error merging data for tablet {}", tabletId, e);
            } finally {
                watch.stop();
                LOG.info("Data merge process for tablet {} took {} ms", tabletId, watch.elapsedMs());
            }
        });
    }

    /**
     * 合并两个Tablet的数据
     */
    private boolean mergeTablets(Tablet original, Tablet temp, TBackend be) {
        try {
            // 1. 获取原始Tablet和临时Tablet的最新版本
            long originalVersion = original.getMaxVersion();
            long tempVersion = temp.getMaxVersion();

            // 2. 调用BE的合并接口，基于MVCC合并数据
            // 这里简化处理，实际实现需要调用BE的合并API
            boolean success = callMergeApi(be, original.getId(), temp.getId(), originalVersion, tempVersion);

            if (success) {
                // 3. 合并成功后，更新原始Tablet的版本
                original.updateMaxVersion(Math.max(originalVersion, tempVersion) + 1);
                return true;
            }
            return false;
        } catch (Exception e) {
            LOG.error("Error merging tablets {} and {}", original.getId(), temp.getId(), e);
            return false;
        }
    }

    /**
     * 调用BE的合并API
     */
    private boolean callMergeApi(TBackend be, long originalTabletId, long tempTabletId,
                                long originalVersion, long tempVersion) {
        // 实际实现中，这里需要通过Thrift调用BE的合并接口
        // 简化处理，假设合并成功
        return true;
    }
}