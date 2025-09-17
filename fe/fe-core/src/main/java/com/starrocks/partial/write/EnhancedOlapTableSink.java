package com.starrocks.partial.write;

import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.StarRocksException;
import com.starrocks.partial.FailureDetector;
import com.starrocks.partial.TempTabletManager;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.server.GlobalStateMgr;
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
import java.util.stream.Collectors;

public class EnhancedOlapTableSink extends OlapTableSink {
    private static final Logger LOG = LogManager.getLogger(EnhancedOlapTableSink.class);

    private final FailureDetector failureDetector;
    private final AsyncTempTabletCreator tempTabletCreator;
    private final TempTabletManager tempTabletManager;
    private final Map<Long, TempTablet> tempTabletMap = new HashMap<>();

    public EnhancedOlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, List<Long> partitionIds,
                                 TWriteQuorumType writeQuorum, boolean enableReplicatedStorage,
                                 boolean nullExprInAutoIncrement, boolean enableAutomaticPartition) {
        super(dstTable, tupleDescriptor, partitionIds, writeQuorum, enableReplicatedStorage,
                nullExprInAutoIncrement, enableAutomaticPartition);
        this.failureDetector = GlobalStateMgr.getCurrentState().getFailureDetector();
        this.tempTabletCreator = GlobalStateMgr.getCurrentState().getAsyncTempTabletCreator();
        this.tempTabletManager = GlobalStateMgr.getCurrentState().getTempTabletManager();
    }

    @Override
    public void complete() throws StarRocksException {
        super.complete();

        TDataSink tDataSink = toThrift();
        TOlapTableSink tSink = tDataSink.getOlap_table_sink();
        TOlapTablePartitionParam partitionParam = tSink.getPartition();

        // 1. Identify failed tablets and create temporary replacements
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (TOlapTablePartition tPartition : partitionParam.getPartitions()) {
            PhysicalPartition physicalPartition = getDstTable().getPhysicalPartition(tPartition.getId());
            physicalPartition.getMaterializedIndices(IndexExtState.ALL).forEach(index -> {
                for (Tablet tablet : index.getTablets()) {
                    if (failureDetector.isTabletFailed(tablet.getId())) {
                        CompletableFuture<Void> future = tempTabletCreator.createTempTabletAsync(tablet.getId())
                                .thenAccept(tempTablet -> {
                                    synchronized (tempTabletMap) {
                                        tempTabletMap.put(tablet.getId(), tempTablet);
                                        tempTabletManager.addTempTablet(tablet.getId(), tempTablet);
                                    }
                                });
                        futures.add(future);
                    }
                }
            });
        }

        if (futures.isEmpty()) {
            return;
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            throw new StarRocksException("Failed to create temporary tablets", e);
        }

        // 2. Rebuild the location parameter
        TOlapTableLocationParam newLocationParam = new TOlapTableLocationParam();
        TOlapTableLocationParam oldLocationParam = tSink.getLocation();
        if (oldLocationParam != null) {
            for (TTabletLocation oldLocation : oldLocationParam.getTablets()) {
                long originalTabletId = oldLocation.getTablet_id();
                if (tempTabletMap.containsKey(originalTabletId)) {
                    TempTablet tempTablet = tempTabletMap.get(originalTabletId);
                    OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getDb(getDstTable().getDbId())
                            .getTable(getDstTable().getId());
                    Partition partition = table.getTempPartition(tempTablet.getTempPartitionId());
                    Tablet tablet = partition.getBaseIndex().getTablet(tempTablet.getTempTabletId());
                    List<Long> backendIds = tablet.getBackendIds();
                    List<TNetworkAddress> addresses = backendIds.stream()
                            .map(beId -> GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(beId))
                            .map(backend -> new TNetworkAddress(backend.getHost(), backend.getBePort()))
                            .collect(Collectors.toList());
                    TTabletLocation newLocation = new TTabletLocation(tempTablet.getTempTabletId(), addresses);
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
