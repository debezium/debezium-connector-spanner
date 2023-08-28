/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.proto;

import static java.util.stream.Collectors.toList;

import java.util.List;

import io.debezium.connector.spanner.kafka.event.proto.SyncEventProtos;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * Maps the TaskSyncEvent class to the SyncEventProtos.SyncEvent protocol buffer, which is the
 * storage format used for the internal Sync Topic.
 */
public class SyncEventToProtoMapper {
    private SyncEventToProtoMapper() {
    }

    public static SyncEventProtos.SyncEvent mapToProto(TaskSyncEvent taskSyncEvent) {

        List<SyncEventProtos.TaskState> protoStates = taskSyncEvent.getTaskStates().values().stream()
                .map(
                        state -> SyncEventProtos.TaskState.newBuilder()
                                .setTaskUid(state.getTaskUid())
                                .setConsumerId(state.getConsumerId())
                                .setRebalanceGenerationId(state.getRebalanceGenerationId())
                                .setStateTimestamp(state.getStateTimestamp())
                                .addAllPartitions(
                                        state.getPartitions().stream()
                                                .distinct()
                                                .map(SyncEventToProtoMapper::mapPartition)
                                                .collect(toList()))
                                .addAllSharedPartitions(
                                        state.getSharedPartitions().stream()
                                                .distinct()
                                                .map(SyncEventToProtoMapper::mapPartition)
                                                .collect(toList()))
                                .build())
                .collect(toList());

        return SyncEventProtos.SyncEvent.newBuilder()
                .setTaskUid(taskSyncEvent.getTaskUid())
                .setConsumerId(taskSyncEvent.getConsumerId())
                .setMessageTimestamp(taskSyncEvent.getMessageTimestamp())
                .setMessageType(
                        SyncEventProtos.MessageType.forNumber(
                                taskSyncEvent.getMessageType() == null
                                        ? 0
                                        : taskSyncEvent.getMessageType().ordinal()))
                .setRebalanceGenerationId(taskSyncEvent.getRebalanceGenerationId())
                .setEpochOffset(taskSyncEvent.getEpochOffset())
                .addAllTaskStates(protoStates)
                .build();
    }

    private static SyncEventProtos.PartitionState mapPartition(PartitionState partitionState) {
        SyncEventProtos.PartitionState.Builder builder = SyncEventProtos.PartitionState.newBuilder()
                .setToken(partitionState.getToken())
                .addAllParents(partitionState.getParents())
                .setStartTimestamp(partitionState.getStartTimestamp().toString())
                .setState(SyncEventProtos.State.forNumber(partitionState.getState().ordinal()))
                .setAssigneeTaskUid(partitionState.getAssigneeTaskUid());

        if (partitionState.getOriginParent() != null) {
            builder.setOriginParent(partitionState.getOriginParent());
        }

        if (partitionState.getEndTimestamp() != null) {
            builder.setEndTimestamp(partitionState.getEndTimestamp().toString());
        }

        if (partitionState.getFinishedTimestamp() != null) {
            builder.setFinishedTimestamp(partitionState.getFinishedTimestamp().toString());
        }

        return builder.build();
    }
}