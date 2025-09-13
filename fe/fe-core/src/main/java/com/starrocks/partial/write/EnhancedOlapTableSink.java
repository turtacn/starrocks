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

package com.starrocks.partial.write;

import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.planner.TupleDescriptor;
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
import com.starrocks.thrift.TTabletLocation;
import com.starrocks.thrift.TWriteQuorumType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EnhancedOlapTableSink extends OlapTableSink {
    private static final Logger LOG = LogManager.getLogger(EnhancedOlapTableSink.class);

    private final FailureDetector failureDetector;
    private final AsyncTempTabletCreator tempTabletCreator;
    // The design document calls for a WriteRequestBuffer, but OlapTableSink does not have a per-batch `send` method
    // on the FE. The planning happens once. So, we will make the creation of temp tablets concurrent, but the
    // overall `complete()` method will block until they are all created.
    private final Map<Long, TempTablet> tempTabletMap = new ConcurrentHashMap<>();

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
        // First, let the parent class do its work to initialize the sink.
        super.complete();

        // After parent `complete()`, the tDataSink is not null.
        TDataSink tDataSink = toThrift();
        TOlapTablePartitionParam partitionParam = tDataSink.getOlap_table_sink().getPartition();
        if (partitionParam == null || partitionParam.getPartitions() == null) {
            LOG.info("No partitions to check for partial writes.");
            return;
        }

        // 1. Identify all failed tablets that need temporary replacements.
        List<CompletableFuture<Void>> creationFutures = new ArrayList<>();
        for (TOlapTablePartition tPartition : partitionParam.getPartitions()) {
            PhysicalPartition physicalPartition = getDstTable().getPhysicalPartition(tPartition.getId());
            if (physicalPartition == null) {
                continue;
            }
            physicalPartition.getMaterializedIndices(IndexExtState.ALL).forEach(index -> {
                for (Tablet tablet : index.getTablets()) {
                    if (failureDetector.isTabletFailed(tablet.getId())) {
                        LOG.warn("Tablet {} is on a failed backend. Creating temporary tablet.", tablet.getId());
                        CompletableFuture<TempTablet> future = tempTabletCreator.createTempTabletAsync(tablet.getId(),
                                getDstTable().getSchemaByIndexId(getDstTable().getBaseIndexId()));

                        creationFutures.add(future.thenAccept(tempTablet -> {
                            if (tempTablet != null) {
                                tempTabletMap.put(tablet.getId(), tempTablet);
                                LOG.info("Successfully created temporary tablet {} for original tablet {}",
                                        tempTablet.getTempTabletId(), tablet.getId());
                            }
                        }));
                    }
                }
            });
        }

        // If no tablets failed, there's nothing more to do.
        if (creationFutures.isEmpty()) {
            LOG.debug("No failed tablets found for this sink.");
            return;
        }

        // 2. Wait for all temporary tablets to be created concurrently.
        LOG.info("Waiting for {} temporary tablets to be created...", creationFutures.size());
        try {
            CompletableFuture.allOf(creationFutures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            LOG.error("Failed to create one or more temporary tablets", e);
            throw new StarRocksException("Failed to create temporary tablets for partially available write", e);
        }
        LOG.info("All temporary tablets created.");

        // 3. Rebuild the sink's location and partition parameters with the new temporary tablet info.
        // We need to modify the thrift object that was already created by the parent's `complete()` method.
        TOlapTableLocationParam oldLocationParam = tDataSink.getOlap_table_sink().getLocation();
        if (oldLocationParam != null) {
            TOlapTableLocationParam newLocationParam = new TOlapTableLocationParam();
            for (TTabletLocation oldLocation : oldLocationParam.getTablets()) {
                long originalTabletId = oldLocation.getTablet_id();
                if (tempTabletMap.containsKey(originalTabletId)) {
                    TempTablet tempTablet = tempTabletMap.get(originalTabletId);
                    // Assuming MockTempTablet for now, which has a single backendId.
                    TTabletLocation newLocation = new TTabletLocation(tempTablet.getTempTabletId(),
                            List.of(tempTablet.getBackendId()));
                    newLocationParam.addToTablets(newLocation);
                } else {
                    newLocationParam.addToTablets(oldLocation);
                }
            }
            tDataSink.getOlap_table_sink().setLocation(newLocationParam);
        }

        for (TOlapTablePartition tPartition : partitionParam.getPartitions()) {
            for (TOlapTableIndexTablets tIndex : tPartition.getIndexes()) {
                List<Long> newTabletIds = tIndex.getTablets().stream()
                        .map(tabletId -> tempTabletMap.containsKey(tabletId)
                                ? tempTabletMap.get(tabletId).getTempTabletId() : tabletId)
                        .collect(Collectors.toList());
                tIndex.setTablets(newTabletIds);
            }
        }
        LOG.info("Successfully rerouted sink to temporary tablets.");
    }
}
