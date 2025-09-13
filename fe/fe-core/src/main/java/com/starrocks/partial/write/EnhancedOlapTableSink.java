package com.starrocks.partial.write;

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.StarRocksException;
import com.starrocks.partial.FailureDetector;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TOlapTableIndexTablets;
import com.starrocks.thrift.TOlapTableLocationParam;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TOlapTablePartitionParam;
import com.starrocks.thrift.TOlapTableSink;
import com.starrocks.thrift.TTabletLocation;
import com.starrocks.thrift.TWriteQuorumType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class EnhancedOlapTableSink extends OlapTableSink {
    private static final Logger LOG = LogManager.getLogger(EnhancedOlapTableSink.class);

    private final FailureDetector failureDetector;
    private final AsyncTempTabletCreator tempTabletCreator;
    private final Map<Long, MockTempTablet> tempTabletMap = new HashMap<>();

    public EnhancedOlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, List<Long> partitionIds,
                                 TWriteQuorumType writeQuorum, boolean enableReplicatedStorage,
                                 boolean nullExprInAutoIncrement, boolean enableAutomaticPartition,
                                 FailureDetector failureDetector, AsyncTempTabletCreator tempTabletCreator) {
        super(dstTable, tupleDescriptor, partitionIds, writeQuorum, enableReplicatedStorage,
                nullExprInAutoIncrement, enableAutomaticPartition);
        this.failureDetector = failureDetector;
        this.tempTabletCreator = tempTabletCreator;
    }

    @Override
    public void complete() throws StarRocksException {
        super.complete();

        TDataSink tDataSink = toThrift();
        TOlapTableSink tSink = tDataSink.getOlap_table_sink();
        TOlapTablePartitionParam partitionParam = tSink.getPartition();

        // 1. Identify failed tablets and create temporary replacements
        for (TOlapTablePartition tPartition : partitionParam.getPartitions()) {
            PhysicalPartition physicalPartition = getDstTable().getPhysicalPartition(tPartition.getId());
            physicalPartition.getMaterializedIndices(IndexExtState.ALL).forEach(index -> {
                for (Tablet tablet : index.getTablets()) {
                    if (failureDetector.isTabletFailed(tablet.getId())) {
                        CompletableFuture<TempTablet> future = tempTabletCreator.createTempTabletAsync(tablet.getId(),
                                getDstTable().getSchemaByIndexId(getDstTable().getBaseIndexId()));
                        try {
                            MockTempTablet tempTablet = (MockTempTablet) future.get();
                            tempTabletMap.put(tablet.getId(), tempTablet);
                        } catch (Exception e) {
                            throw new RuntimeException(new StarRocksException("Failed to create temporary tablet", e));
                        }
                    }
                }
            });
        }

        if (tempTabletMap.isEmpty()) {
            return;
        }

        // 2. Rebuild the location parameter
        TOlapTableLocationParam newLocationParam = new TOlapTableLocationParam();
        TOlapTableLocationParam oldLocationParam = tSink.getLocation();
        if (oldLocationParam != null) {
            for (TTabletLocation oldLocation : oldLocationParam.getTablets()) {
                long originalTabletId = oldLocation.getTablet_id();
                if (tempTabletMap.containsKey(originalTabletId)) {
                    MockTempTablet tempTablet = tempTabletMap.get(originalTabletId);
                    TTabletLocation newLocation = new TTabletLocation(tempTablet.getTempTabletId(), List.of(tempTablet.getBackendId()));
                    newLocationParam.addToTablets(newLocation);
                } else {
                    newLocationParam.addToTablets(oldLocation);
                }
            }
            tSink.setLocation(newLocationParam);
        }


        // 3. Rebuild the partition parameter
        for (TOlapTablePartition tPartition : partitionParam.getPartitions()) {
            for (TOlapTableIndexTablets tIndex : tPartition.getIndexes()) {
                List<Long> newTabletIds = new ArrayList<>();
                for (long tabletId : tIndex.getTablets()) {
                    if (tempTabletMap.containsKey(tabletId)) {
                        newTabletIds.add(tempTabletMap.get(tabletId).getTempTabletId());
                    } else {
                        newTabletIds.add(tabletId);
                    }
                }
                tIndex.setTablets(newTabletIds);
            }
        }
    }
}
