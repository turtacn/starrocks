package com.starrocks.partial.failure;

import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.BackendService;
import com.starrocks.thrift.TBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FailureDetector {
    private static final Logger LOG = LoggerFactory.getLogger(FailureDetector.class);
    private static FailureDetector instance;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final FailureRepository repository;

    private FailureDetector() {
        this.repository = new FailureRepository();
        // 启动定时检测任务
        if (Config.partial_available_enabled) {
            scheduler.scheduleAtFixedRate(this::detectFailures, 0,
                    Config.tablet_sched_checker_interval_seconds, TimeUnit.SECONDS);
            LOG.info("Partial available mode enabled, failure detector started");
        }
    }

    public static synchronized FailureDetector getInstance() {
        if (instance == null) {
            instance = new FailureDetector();
        }
        return instance;
    }

    /**
     * 检测故障BE节点和Tablet
     */
    private void detectFailures() {
        try {
            // 获取所有BE节点
            List<TBackend> backends = BackendService.getInstance().getAllBackends();

            for (TBackend backend : backends) {
                // 检查BE是否存活
                boolean isAlive = BackendService.getInstance().isBackendAlive(backend.backend_id);

                // 获取该BE上的所有Tablet
                List<Tablet> tablets = GlobalStateMgr.getCurrentState().getTabletInBackend(backend.backend_id);

                if (!isAlive) {
                    // BE节点宕机，标记其上所有Tablet为故障
                    for (Tablet tablet : tablets) {
                        if (!repository.isTabletFailed(tablet.getId())) {
                            LOG.warn("Tablet {} on backend {} is marked as failed",
                                    tablet.getId(), backend.backend_id);
                            repository.markTabletFailed(tablet, backend.backend_id);
                        }
                    }
                } else {
                    // BE节点恢复，标记其上所有Tablet为恢复中
                    for (Tablet tablet : tablets) {
                        if (repository.isTabletFailed(tablet.getId())) {
                            LOG.info("Tablet {} on backend {} is recovering",
                                    tablet.getId(), backend.backend_id);
                            repository.markTabletRecovering(tablet.getId());
                            // 触发数据合并
                            triggerDataMerge(tablet.getId());
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error detecting failures", e);
        }
    }

    /**
     * 触发数据合并
     */
    private void triggerDataMerge(long tabletId) {
        // 调用数据合并模块
        DataMerger.getInstance().mergeTabletData(tabletId);
    }

    /**
     * 获取表的故障Tablet
     */
    public List<Long> getFailedTabletsByTable(long tableId) {
        return repository.getFailedTabletsByTable(tableId);
    }

    /**
     * 检查Tablet是否故障
     */
    public boolean isTabletFailed(long tabletId) {
        return repository.isTabletFailed(tabletId);
    }

    /**
     * 获取故障Tablet对应的临时表ID
     */
    public Long getTempTableId(long tabletId) {
        return repository.getTempTableId(tabletId);
    }

    /**
     * 标记Tablet为已恢复
     */
    public void markTabletRecovered(long tabletId) {
        repository.markTabletRecovered(tabletId);
    }
}