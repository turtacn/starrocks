package com.starrocks.partial.query;

import com.starrocks.analysis.QueryStmt;
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

    public void filter(QueryStmt queryStmt, PlanNode plan) {
        // This is a placeholder for the query filtering logic.
        // The actual implementation would involve traversing the plan,
        // finding the OlapScanNodes, and removing the tablets that are on failed BEs.
        LOG.info("Applying query filter...");

        List<OlapScanNode> scanNodes = findOlapScanNodes(plan);
        for (OlapScanNode scanNode : scanNodes) {
            try {
                List<Long> originalTabletIds = scanNode.getScanTabletIds();
                List<Long> filteredTabletIds = originalTabletIds.stream()
                        .filter(tabletId -> !failureDetector.isTabletFailed(tabletId))
                        .collect(Collectors.toList());

                if (originalTabletIds.size() != filteredTabletIds.size()) {
                    LOG.warn("Query filtered. Original tablets: {}, Filtered tablets: {}",
                            originalTabletIds, filteredTabletIds);
                    // There is no public setter for the tablet IDs on OlapScanNode.
                    // A proper hook mechanism would be needed to apply this filter.
                    // For now, this method only logs the filtering action.
                    // A possible solution is to use reflection to modify the private field,
                    // but that is not a good practice.
                    // Another solution is to modify the OlapScanNode class to add a setter,
                    // but that would be an intrusive change.
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
