package com.starrocks.partial.query;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ScanNode;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.partial.failure.FailureDetector;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.Planner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryFilter {
    private static final Logger LOG = LoggerFactory.getLogger(QueryFilter.class);
    private static QueryFilter instance;
    private final FailureDetector failureDetector;

    private QueryFilter() {
        this.failureDetector = FailureDetector.getInstance();
    }

    public static synchronized QueryFilter getInstance() {
        if (instance == null) {
            instance = new QueryFilter();
        }
        return instance;
    }

    /**
     * 过滤查询计划中的故障Tablet
     */
    public boolean filterQueryPlan(Planner planner) throws AnalysisException {
        if (!Config.partial_available_enabled) {
            return true;
        }

        List<PlanFragment> fragments = planner.getFragments();
        boolean hasFailedTablets = false;
        boolean queryCanProceed = true;

        for (PlanFragment fragment : fragments) {
            List<ScanNode> scanNodes = fragment.getScanNodes();
            for (ScanNode scanNode : scanNodes) {
                if (scanNode instanceof OlapScanNode) {
                    OlapScanNode olapScan = (OlapScanNode) scanNode;
                    OlapTable table = olapScan.getTable();

                    // 获取该表的所有故障Tablet
                    List<Long> failedTablets = failureDetector.getFailedTabletsByTable(table.getId());
                    if (!failedTablets.isEmpty()) {
                        hasFailedTablets = true;
                        LOG.info("Query involves failed tablets for table {}: {}",
                                table.getName(), failedTablets);

                        // 获取查询涉及的Tablet
                        Set<Long> queryTablets = olapScan.getTabletIds();

                        // 检查是否所有Tablet都故障
                        if (queryTablets.stream().allMatch(failedTablets::contains)) {
                            // 查询无法执行
                            queryCanProceed = false;
                            break;
                        }

                        // 过滤掉故障Tablet
                        Set<Long> healthyTablets = queryTablets.stream()
                                .filter(t -> !failedTablets.contains(t))
                                .collect(Collectors.toSet());

                        // 更新扫描节点的Tablet列表
                        olapScan.setTabletIds(healthyTablets);

                        // 如果过滤后没有可用Tablet，查询无法执行
                        if (healthyTablets.isEmpty()) {
                            queryCanProceed = false;
                            break;
                        }
                    }
                }
            }
            if (!queryCanProceed) {
                break;
            }
        }

        if (hasFailedTablets) {
            if (queryCanProceed) {
                // 查询已被裁剪，添加警告信息
                planner.addWarning("Query results may be incomplete due to some tablets being unavailable");
            } else {
                // 查询无法执行
                throw new AnalysisException("Query cannot be executed because all required tablets are unavailable");
            }
        }

        return queryCanProceed;
    }
}