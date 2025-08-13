package com.starrocks.partial.write;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Pair;
import com.starrocks.load.stream.StreamLoadRouter;
import com.starrocks.partial.failure.FailureDetector;
import com.starrocks.rpc.BEChannel;
import com.starrocks.rpc.BEProxy;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStreamLoadPutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class WriteRouter {
    private static final Logger LOG = LoggerFactory.getLogger(WriteRouter.class);
    private static WriteRouter instance;
    private final FailureDetector failureDetector;
    private final StreamLoadRouter originalRouter;

    private WriteRouter() {
        this.failureDetector = FailureDetector.getInstance();
        this.originalRouter = new StreamLoadRouter();
    }

    public static synchronized WriteRouter getInstance() {
        if (instance == null) {
            instance = new WriteRouter();
        }
        return instance;
    }

    /**
     * 路由写入请求到合适的Tablet
     */
    public Pair<BEChannel, TStreamLoadPutRequest> routeWriteRequest(
            OlapTable table, List<Object> partitionKeys, List<Object> bucketKeys) {
        if (!Config.partial_available_enabled) {
            // 功能未启用，使用原始路由
            return originalRouter.route(table, partitionKeys, bucketKeys);
        }

        // 计算原始目标Tablet
        Pair<BEChannel, TStreamLoadPutRequest> originalRoute =
                originalRouter.route(table, partitionKeys, bucketKeys);

        // 获取Tablet ID
        long tabletId = originalRoute.second.tablet_id;

        if (!failureDetector.isTabletFailed(tabletId)) {
            // Tablet健康，使用原始路由
            return originalRoute;
        }

        // Tablet故障，路由到临时表
        LOG.warn("Tablet {} is failed, routing to temp table", tabletId);

        Long tempTableId = failureDetector.getTempTableId(tabletId);
        if (tempTableId == null) {
            LOG.error("No temp table found for failed tablet {}", tabletId);
            throw new RuntimeException("Failed to route write request: no temp table available");
        }

        // 获取临时表
        OlapTable tempTable = (OlapTable) GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(tempTableId);
        if (tempTable == null) {
            LOG.error("Temp table {} not found for failed tablet {}", tempTableId, tabletId);
            throw new RuntimeException("Failed to route write request: temp table not found");
        }

        // 路由到临时表
        return originalRouter.route(tempTable, partitionKeys, bucketKeys);
    }
}