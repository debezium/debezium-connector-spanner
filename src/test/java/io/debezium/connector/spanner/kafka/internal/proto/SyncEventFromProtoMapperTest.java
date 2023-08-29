/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.proto;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.event.proto.SyncEventProtos;
import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

class SyncEventFromProtoMapperTest {

    @Test
    void testMapFromProto() {
        var protoPartition1 = SyncEventProtos.PartitionState.newBuilder()
                .setToken("aqaqa098----08989")
                .addAllParents(List.of("p1", "p2"))
                .setStartTimestamp("1970-01-01T00:00:00.000002000Z")
                .setState(SyncEventProtos.State.CREATED)
                .setAssigneeTaskUid("2llll")
                .setEndTimestamp("1970-01-01T00:00:00.000001000Z")
                .build();

        var protoPartition2 = SyncEventProtos.PartitionState.newBuilder()
                .setToken("eeeea098----08989")
                .addAllParents(List.of("p4", "p7"))
                .setStartTimestamp("1970-01-01T00:00:00.000007000Z")
                .setState(SyncEventProtos.State.READY_FOR_STREAMING)
                .setAssigneeTaskUid("2llll")
                .setEndTimestamp("1970-01-01T00:00:00.000008000Z")
                .build();

        var protoState1 = SyncEventProtos.TaskState.newBuilder()
                .setTaskUid("uienjnjaaaa")
                .setConsumerId("ppp323323")
                .setRebalanceGenerationId(7878)
                .setStateTimestamp(11111)
                .addAllPartitions(List.of(protoPartition1, protoPartition2))
                .addAllSharedPartitions(List.of(protoPartition2))
                .build();

        var protoState2 = SyncEventProtos.TaskState.newBuilder()
                .setTaskUid("ttytgtg")
                .setConsumerId("hhhj3344")
                .setRebalanceGenerationId(45444)
                .setStateTimestamp(111166661)
                .addAllPartitions(List.of(protoPartition2))
                .addAllSharedPartitions(List.of())
                .build();

        var protoEvent = SyncEventProtos.SyncEvent.newBuilder()
                .setTaskUid("t2283238")
                .setConsumerId("c2323399800")
                .setMessageTimestamp(123989)
                .setMessageType(
                        SyncEventProtos.MessageType.REBALANCE_ANSWER)
                .setRebalanceGenerationId(90)
                .setEpochOffset(123)
                .addAllTaskStates(List.of(protoState1, protoState2))
                .build();

        TaskSyncEvent taskSyncEvent = SyncEventFromProtoMapper.mapFromProto(protoEvent);

        assertThat(taskSyncEvent.getTaskUid()).isEqualTo(protoEvent.getTaskUid());
        assertThat(taskSyncEvent.getConsumerId()).isEqualTo(protoEvent.getConsumerId());
        assertThat(taskSyncEvent.getMessageTimestamp()).isEqualTo(protoEvent.getMessageTimestamp());
        assertThat(taskSyncEvent.getMessageType()).isEqualTo(MessageTypeEnum.REBALANCE_ANSWER);
        assertThat(taskSyncEvent.getRebalanceGenerationId()).isEqualTo(protoEvent.getRebalanceGenerationId());
        assertThat(taskSyncEvent.getEpochOffset()).isEqualTo(protoEvent.getEpochOffset());
        assertThat(taskSyncEvent.getTaskStates()).hasSize(2);

        TaskState taskState1 = taskSyncEvent.getTaskStates().get(protoState1.getTaskUid());
        assertThat(taskState1.getTaskUid()).isEqualTo(protoState1.getTaskUid());
        assertThat(taskState1.getConsumerId()).isEqualTo(protoState1.getConsumerId());
        assertThat(taskState1.getRebalanceGenerationId()).isEqualTo(protoState1.getRebalanceGenerationId());
        assertThat(taskState1.getStateTimestamp()).isEqualTo(protoState1.getStateTimestamp());

        assertThat(taskState1.getPartitionsMap()).hasSize(2);
        PartitionState partition1 = taskState1.getPartitionsMap().get(protoPartition1.getToken());
        assertPartition(partition1, protoPartition1, PartitionStateEnum.CREATED);
        PartitionState partition2 = taskState1.getPartitionsMap().get(protoPartition2.getToken());
        assertPartition(partition2, protoPartition2, PartitionStateEnum.READY_FOR_STREAMING);

        assertThat(taskState1.getSharedPartitions()).hasSize(1);
        PartitionState shared2 = taskState1.getPartitionsMap().get(protoPartition2.getToken());
        assertPartition(shared2, protoPartition2, PartitionStateEnum.READY_FOR_STREAMING);

        TaskState taskState2 = taskSyncEvent.getTaskStates().get(protoState2.getTaskUid());
        assertThat(taskState2.getTaskUid()).isEqualTo(protoState2.getTaskUid());
        assertThat(taskState2.getConsumerId()).isEqualTo(protoState2.getConsumerId());
        assertThat(taskState2.getRebalanceGenerationId()).isEqualTo(protoState2.getRebalanceGenerationId());
        assertThat(taskState2.getStateTimestamp()).isEqualTo(protoState2.getStateTimestamp());

        assertThat(taskState2.getPartitionsMap()).hasSize(1);
        PartitionState partition22 = taskState2.getPartitionsMap().get(protoPartition2.getToken());
        assertPartition(partition22, protoPartition2, PartitionStateEnum.READY_FOR_STREAMING);
        assertThat(taskState2.getSharedPartitionsMap()).isEmpty();
    }

    private void assertPartition(PartitionState actual, SyncEventProtos.PartitionState expected, PartitionStateEnum partitionState) {
        assertThat(actual.getToken()).isEqualTo(expected.getToken());
        assertThat(actual.getParents()).containsExactlyInAnyOrderElementsOf(expected.getParentsList());
        assertThat(actual.getStartTimestamp()).isEqualTo(Timestamp.parseTimestamp(expected.getStartTimestamp()));
        assertThat(actual.getState()).isEqualTo(partitionState);
        assertThat(actual.getAssigneeTaskUid()).isEqualTo(expected.getAssigneeTaskUid());
        assertThat(actual.getEndTimestamp()).isEqualTo(Timestamp.parseTimestamp(expected.getEndTimestamp()));
    }
}