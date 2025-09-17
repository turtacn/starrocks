package com.starrocks.partial.write;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.SingleRangePartitionDesc;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class AsyncTempTabletCreator {

    public CompletableFuture<TempTablet> createTempTabletAsync(long originalTabletId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
                TabletInvertedIndex invertedIndex = gsm.getTabletInvertedIndex();
                long tableId = invertedIndex.getTableId(originalTabletId);
                Database db = gsm.getDb(invertedIndex.getDbId(originalTabletId));
                if (db == null) {
                    throw new DdlException("Database not found for tablet " + originalTabletId);
                }
                OlapTable table = (OlapTable) db.getTable(tableId);
                if (table == null) {
                    throw new DdlException("Table not found for tablet " + originalTabletId);
                }

                String tempPartitionName = "__temp_partial_" + originalTabletId;
                if (table.getTempPartition(tempPartitionName) != null) {
                    // temp partition already exists, just return it.
                    Partition tempPartition = table.getTempPartition(tempPartitionName);
                    return new TempTablet(tempPartition.getId(), tempPartition.getBaseIndex().getTablets().get(0).getId());
                }

                // For simplicity, we only support range-partitioned tables for now, and we create a new partition
                // that covers a very large range to avoid conflicts.
                // A more robust solution would be to create a partition with a range that is specific to the failed tablet.
                PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(Collections.singletonList(new PartitionValue(Long.MAX_VALUE)));
                SingleRangePartitionDesc partitionDesc = new SingleRangePartitionDesc(
                        true,
                        tempPartitionName,
                        partitionKeyDesc,
                        Maps.newHashMap());

                AddPartitionClause addPartitionClause = new AddPartitionClause(
                        partitionDesc,
                        table.getDefaultDistributionInfo().copy(),
                        Maps.newHashMap(),
                        true
                );

                gsm.getLocalMetastore().addPartitions(ConnectContext.get(), db, table.getName(), addPartitionClause);

                Partition tempPartition = table.getTempPartition(tempPartitionName);
                if (tempPartition == null) {
                    throw new DdlException("Failed to create temp partition " + tempPartitionName);
                }
                long tempTabletId = tempPartition.getBaseIndex().getTablets().get(0).getId();

                return new TempTablet(tempPartition.getId(), tempTabletId);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }
}
