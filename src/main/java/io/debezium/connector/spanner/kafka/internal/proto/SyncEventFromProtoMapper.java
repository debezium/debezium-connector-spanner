/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.proto;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.event.proto.SyncEventProtos;
import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * Maps the SyncEventProtos.SyncEvent protocol buffer to TaskSyncEvent class
 */
public class SyncEventFromProtoMapper {
    private SyncEventFromProtoMapper() {
    }

    public static TaskSyncEvent mapFromProto(SyncEventProtos.SyncEvent protoEvent) {
        List<SyncEventProtos.TaskState> protoStates = protoEvent.getTaskStatesList();

        Map<String, TaskState> connectorStates = new HashMap<>(protoStates.size());
        for (int i = 0; i < protoEvent.getTaskStatesCount(); i++) {
            var protoState = protoEvent.getTaskStates(i);
            var taskState = new TaskState(
                    protoState.getTaskUid(),
                    protoState.getConsumerId(),
                    protoState.getRebalanceGenerationId(),
                    protoState.getStateTimestamp(),
                    mapPartitionsArray(protoState),
                    mapSharedPartitionsArray(protoState));
            connectorStates.put(taskState.getTaskUid(), taskState);
        }

        return new TaskSyncEvent(
                protoEvent.getTaskUid(),
                protoEvent.getConsumerId(),
                protoEvent.getMessageTimestamp(),
                MessageTypeEnum.valueOf(protoEvent.getMessageType().name()),
                protoEvent.getRebalanceGenerationId(),
                protoEvent.getEpochOffset(),
                connectorStates);
    }

    private static Map<String, PartitionState> mapPartitionsArray(SyncEventProtos.TaskState protoState) {
        Map<String, PartitionState> partitions = new HashMap<>(protoState.getPartitionsCount());

        for (int i = 0; i < protoState.getPartitionsCount(); i++) {
            var protoPartition = protoState.getPartitions(i);
            var partition = mapPartition(protoPartition);
            partitions.put(partition.getToken(), partition);
        }

        return partitions;
    }

    private static Map<String, PartitionState> mapSharedPartitionsArray(SyncEventProtos.TaskState protoState) {
        Map<String, PartitionState> partitions = new HashMap<>(protoState.getSharedPartitionsCount());

        for (int i = 0; i < protoState.getSharedPartitionsCount(); i++) {
            var protoPartition = protoState.getSharedPartitions(i);
            var partition = mapPartition(protoPartition);
            partitions.put(partition.getToken(), partition);
        }

        return partitions;
    }

    private static PartitionState mapPartition(SyncEventProtos.PartitionState partitionState) {

        return new PartitionState(
                partitionState.getToken(),
                Timestamp.parseTimestamp(partitionState.getStartTimestamp()),
                partitionState.getEndTimestamp() != null && !partitionState.getEndTimestamp().isEmpty()
                        ? Timestamp.parseTimestamp(partitionState.getEndTimestamp())
                        : null,
                PartitionStateEnum.valueOf(partitionState.getState().name()),
                new HashSet<>(partitionState.getParentsList()),
                partitionState.getAssigneeTaskUid(),
                partitionState.getFinishedTimestamp() != null && !partitionState.getFinishedTimestamp().isEmpty()
                        ? Timestamp.parseTimestamp(partitionState.getFinishedTimestamp())
                        : null);
    }
}
