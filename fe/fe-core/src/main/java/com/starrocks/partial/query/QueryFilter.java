package com.starrocks.partial.query;

import com.starrocks.common.StarRocksException;
import com.starrocks.partial.FailureDetector;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.sql.ast.QueryStatement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryFilter {
    private static final Logger LOG = LogManager.getLogger(QueryFilter.class);

    private final FailureDetector failureDetector;

    public QueryFilter(FailureDetector failureDetector) {
        this.failureDetector = failureDetector;
    }

    public void filter(QueryStatement queryStmt, PlanNode plan) throws StarRocksException {
        List<OlapScanNode> scanNodes = findOlapScanNodes(plan);
        for (OlapScanNode scanNode : scanNodes) {
            Set<Long> requiredTablets = scanNode.getScanTabletIds().stream().collect(Collectors.toSet());
            Set<Long> failedTablets = failureDetector.getFailedTablets(scanNode.getOlapTable().getId());

            if (canPruneQuery(requiredTablets, failedTablets)) {
                scanNode.filterTablets(tabletId -> !failureDetector.isTabletFailed(tabletId));
                LOG.info("Query filtered. Original tablets: {}, Filtered tablets: {}",
                        requiredTablets, scanNode.getScanTabletIds());
            } else {
                throw new StarRocksException("Query failed due to unavailable tablets");
            }
        }
    }

    public boolean canPruneQuery(Set<Long> requiredTablets, Set<Long> failedTablets) {
        if (failedTablets == null || failedTablets.isEmpty()) {
            return true;
        }
        return requiredTablets.stream().noneMatch(failedTablets::contains);
    }

    private List<OlapScanNode> findOlapScanNodes(PlanNode plan) {
        List<OlapScanNode> scanNodes = new ArrayList<>();
        findOlapScanNodes(plan, scanNodes);
        return scanNodes;
    }

    private void findOlapScanNodes(PlanNode plan, List<OlapScanNode> scanNodes) {
        if (plan instanceof OlapScanNode) {
            scanNodes.add((OlapScanNode) plan);
        }

        if (plan.getChildren() != null) {
            for (PlanNode child : plan.getChildren()) {
                findOlapScanNodes(child, scanNodes);
            }
        }
    }
}
