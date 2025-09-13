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

package com.starrocks.partial.query;

import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.partial.FailureDetector;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryFilter {
    private static final Logger LOG = LogManager.getLogger(QueryFilter.class);

    private final FailureDetector failureDetector;

    public QueryFilter(FailureDetector failureDetector) {
        this.failureDetector = failureDetector;
    }

    public void filter(QueryStatement queryStmt, PlanNode plan) {
        if (plan == null) {
            return;
        }

        LOG.info("Applying partial availability query filter...");

        List<OlapScanNode> scanNodes = findOlapScanNodes(plan);
        for (OlapScanNode scanNode : scanNodes) {
            try {
                List<Long> originalTabletIds = scanNode.getScanTabletIds();
                if (originalTabletIds == null || originalTabletIds.isEmpty()) {
                    continue;
                }

                scanNode.filterScanTablets(tabletId -> !failureDetector.isTabletFailed(tabletId));

                List<Long> filteredTabletIds = scanNode.getScanTabletIds();

                if (originalTabletIds.size() != filteredTabletIds.size()) {
                    LOG.warn("Query filtered for table {}. Original tablets count: {}, Filtered tablets count: {}",
                            scanNode.getOlapTable().getName(), originalTabletIds.size(), filteredTabletIds.size());
                    // In the future, we might want to add a warning to the ConnectContext
                    // so the user is notified about the partial result.
                }
            } catch (Exception e) {
                LOG.warn("Failed to filter tablets for scan node: " + scanNode.getId(), e);
            }
        }
    }

    private List<OlapScanNode> findOlapScanNodes(PlanNode plan) {
        List<OlapScanNode> scanNodes = new ArrayList<>();
        findOlapScanNodesRecursive(plan, scanNodes);
        return scanNodes;
    }

    private void findOlapScanNodesRecursive(PlanNode plan, List<OlapScanNode> scanNodes) {
        if (plan instanceof OlapScanNode) {
            scanNodes.add((OlapScanNode) plan);
        }

        if (plan.getChildren() != null) {
            for (PlanNode child : plan.getChildren()) {
                findOlapScanNodesRecursive(child, scanNodes);
            }
        }
    }
}
