/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.scale.proto;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.kafka.event.proto.SyncEventProtos;

class SyncEventProtoMessagesTest {

    @Test
    void checkSyncEventProtobufSerializationWorks() throws IOException {

        var taskStateWithData = SyncEventProtos.TaskState.newBuilder()
                .setTaskUid(UUID.randomUUID().toString())
                .setConsumerId(UUID.randomUUID().toString())
                .setStateTimestamp(Instant.now().toEpochMilli())
                .addPartitions(SyncEventProtos.PartitionState.newBuilder()
                        .setToken("test_token1")
                        .build())
                .addSharedPartitions(SyncEventProtos.PartitionState.newBuilder()
                        .setToken("test_token2")
                        .build())
                .build();

        var taskStateWithoutData = SyncEventProtos.TaskState.newBuilder()
                .setTaskUid(UUID.randomUUID().toString())
                .setConsumerId(UUID.randomUUID().toString())
                .setStateTimestamp(Instant.now().plusSeconds(5000).toEpochMilli())
                .build();

        var event = SyncEventProtos.SyncEvent.newBuilder()
                .setTaskUid(taskStateWithData.getTaskUid())
                .setConsumerId(taskStateWithData.getConsumerId())
                .setMessageTimestamp(taskStateWithData.getStateTimestamp())
                .addTaskStates(taskStateWithData)
                .addTaskStates(taskStateWithoutData)
                .build();

        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
            event.writeTo(byteStream);
            SyncEventProtos.SyncEvent deserializedEvent = SyncEventProtos.SyncEvent.newBuilder()
                    .mergeFrom(byteStream.toByteArray())
                    .build();

            assertThat(deserializedEvent).isEqualTo(event);
            assertThat(deserializedEvent.getTaskStatesList())
                    .hasSameSizeAs(event.getTaskStatesList())
                    .hasSameElementsAs(event.getTaskStatesList());

            var deserializedStateWithData = deserializedEvent.getTaskStates(0);
            Assertions.assertEquals(1, deserializedStateWithData.getPartitionsCount());
            Assertions.assertEquals(1, deserializedStateWithData.getSharedPartitionsCount());

            var deserializedStateWithoutData = deserializedEvent.getTaskStates(1);
            Assertions.assertEquals(0, deserializedStateWithoutData.getPartitionsCount());
            Assertions.assertEquals(0, deserializedStateWithoutData.getSharedPartitionsCount());
        }
    }

}
