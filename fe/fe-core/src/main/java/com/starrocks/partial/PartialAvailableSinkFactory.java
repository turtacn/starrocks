package com.starrocks.partial;

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.partial.write.AsyncTempTabletCreator;
import com.starrocks.partial.write.EnhancedOlapTableSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TWriteQuorumType;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.List;

public class PartialAvailableSinkFactory {

    public static OlapTableSink create(OlapTable olapTable, TupleDescriptor tupleDesc, List<Long> targetPartitionIds,
                                       TWriteQuorumType writeQuorum, boolean enableReplicatedStorage,
                                       boolean nullExprInAutoIncrement, boolean enableAutomaticPartition,
                                       ComputeResource computeResource) {
        if (Config.partial_available_enabled) {
            GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
            FailureDetector failureDetector = gsm.getFailureDetector();
            AsyncTempTabletCreator asyncTempTabletCreator = gsm.getAsyncTempTabletCreator();

            return new EnhancedOlapTableSink(olapTable, tupleDesc, targetPartitionIds, writeQuorum,
                    enableReplicatedStorage, nullExprInAutoIncrement, enableAutomaticPartition,
                    failureDetector, asyncTempTabletCreator);
        } else {
            return new OlapTableSink(olapTable, tupleDesc, targetPartitionIds, writeQuorum,
                    enableReplicatedStorage, nullExprInAutoIncrement, enableAutomaticPartition, computeResource);
        }
    }
}
