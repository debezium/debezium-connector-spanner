/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.proto;

import static io.debezium.connector.spanner.task.TaskTestHelper.generateTaskStateWithPartitions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.event.proto.SyncEventProtos;
import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

class SyncEventToProtoMapperTest {

    private static SyncEventProtos.PartitionState mapPartition(PartitionState partitionState) {
        SyncEventProtos.PartitionState.Builder builder = SyncEventProtos.PartitionState.newBuilder()
                .setToken(partitionState.getToken())
                .setState(SyncEventProtos.State.forNumber(partitionState.getState().ordinal()));

        if (!partitionState.getState().equals(PartitionStateEnum.REMOVED)) {
            builder.addAllParents(partitionState.getParents());
            builder.setStartTimestamp(partitionState.getStartTimestamp().toString());
            builder.setAssigneeTaskUid(partitionState.getAssigneeTaskUid());
        }
        // Need to figure out how to handle this corner case here.
        // For REMOVED partitions, we can probabyl choose to ignore the parents / start timestamp / assignee task UID rather than populate fake values.
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

    @Test
    void testMapToProto() {
        HashMap<String, TaskState> taskStates = new HashMap<>();
        TaskState task0 = generateTaskStateWithPartitions("task0", "consumerid1", 0, 0,
                List.of(PartitionState.builder()
                        .token("token1").state(PartitionStateEnum.REMOVED).build(),
                        PartitionState.builder()
                                .token("token2").state(PartitionStateEnum.CREATED)
                                .parents(Collections.singleton("Parent0"))
                                .startTimestamp(Timestamp.now())
                                .assigneeTaskUid("task1")
                                .build()),
                List.of(PartitionState.builder()
                        .token("token3").state(PartitionStateEnum.RUNNING)
                        .parents(Collections.singleton("Parent0"))
                        .startTimestamp(Timestamp.now())
                        .assigneeTaskUid("task1")
                        .build()));
        taskStates.put("task0", task0);

        SyncEventProtos.SyncEvent actualMapToProtoResult = SyncEventToProtoMapper.mapToProto(
                new TaskSyncEvent("12", "23", 1L, MessageTypeEnum.REGULAR, 42L,
                        1L, taskStates));
        assertTrue(actualMapToProtoResult.isInitialized());
        assertEquals(1L, actualMapToProtoResult.getEpochOffset());
        assertEquals(1, actualMapToProtoResult.getTaskStatesCount());
        assertEquals("12", actualMapToProtoResult.getTaskUid());
        assertEquals(1L, actualMapToProtoResult.getMessageTimestamp());
        assertEquals(167, actualMapToProtoResult.getSerializedSize());
        assertEquals(42L, actualMapToProtoResult.getRebalanceGenerationId());
        assertEquals(0, actualMapToProtoResult.getMessageTypeValue());
        assertEquals("23", actualMapToProtoResult.getConsumerId());
        assertEquals(2, actualMapToProtoResult.getTaskStates(0).getPartitionsCount());
        assertEquals(1, actualMapToProtoResult.getTaskStates(0).getSharedPartitionsCount());
    }
}
