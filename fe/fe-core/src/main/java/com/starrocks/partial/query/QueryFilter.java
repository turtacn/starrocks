package com.starrocks.partial.query;

import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.partial.FailureDetector;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryFilter {
    private static final Logger LOG = LogManager.getLogger(QueryFilter.class);

    private final FailureDetector failureDetector;

    public QueryFilter(FailureDetector failureDetector) {
        this.failureDetector = failureDetector;
    }

    public void filter(QueryStatement queryStmt, PlanNode plan) {
        List<OlapScanNode> scanNodes = findOlapScanNodes(plan);
        for (OlapScanNode scanNode : scanNodes) {
            try {
                List<Long> originalTabletIds = new ArrayList<>(scanNode.getScanTabletIds());
                scanNode.filterTablets(tabletId -> !failureDetector.isTabletFailed(tabletId));
                List<Long> filteredTabletIds = scanNode.getScanTabletIds();

                if (originalTabletIds.size() != filteredTabletIds.size()) {
                    LOG.warn("Query filtered. Original tablets: {}, Filtered tablets: {}",
                            originalTabletIds, filteredTabletIds);
                }
            } catch (Exception e) {
                LOG.warn("Failed to filter tablets for scan node: " + scanNode.getId(), e);
            }
        }
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
