package com.starrocks.partial;

import com.google.gson.Gson;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.partial.failure.InMemoryTabletFailureRepository;
import com.starrocks.partial.failure.MySQLTabletFailureRepository;
import com.starrocks.partial.failure.TabletFailure;
import com.starrocks.partial.failure.TabletFailureRepository;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class TabletFailureMgr {
    private static final Logger LOG = LogManager.getLogger(TabletFailureMgr.class);

    private TabletFailureRepository tabletFailureRepository;

    public TabletFailureMgr() {
        if ("mysql".equalsIgnoreCase(Config.partial_availability_storage_type)) {
            this.tabletFailureRepository = new MySQLTabletFailureRepository(
                    Config.partial_availability_jdbc_url,
                    Config.partial_availability_jdbc_user,
                    Config.partial_availability_jdbc_password);
        } else {
            this.tabletFailureRepository = new InMemoryTabletFailureRepository();
        }
    }

    public TabletFailureRepository getTabletFailureRepository() {
        return tabletFailureRepository;
    }

    // save and load methods are for in-memory repository, which persists data in FE metadata.
    // When using MySQL repository, these methods are not needed.
    /*
    public void save(SRMetaBlockWriter writer) throws IOException {
        if (tabletFailureRepository instanceof InMemoryTabletFailureRepository) {
            List<TabletFailure> failures = tabletFailureRepository.getTabletsByStatus(null); // Assuming null gets all
            writer.writeJson(failures);
            LOG.info("Saved {} tablet failures", failures.size());
        }
    }

    public void load(SRMetaBlockReader reader) throws IOException {
        if (tabletFailureRepository instanceof InMemoryTabletFailureRepository) {
            List<TabletFailure> failures = reader.readJsonList(TabletFailure.class);
            for (TabletFailure failure : failures) {
                tabletFailureRepository.save(failure);
            }
            LOG.info("Loaded {} tablet failures", failures.size());
        }
    }
    */

    public void replaySave(TabletFailure tabletFailure) {
        // For in-memory repo, replay is handled by loading from image.
        // For mysql repo, this is not needed as it's persisted externally.
        // However, if we want to support both, we might need to adjust this logic.
        // For now, we assume this is for journal replay for in-memory state changes.
        if (tabletFailureRepository instanceof InMemoryTabletFailureRepository) {
            tabletFailureRepository.save(tabletFailure);
        }
    }
}
