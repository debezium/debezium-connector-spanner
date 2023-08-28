/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;

import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.kafka.event.proto.SyncEventProtos;
import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

class SyncEventToProtoMapperTest {

    @Test
    void testMapToProto() {
        HashMap<String, TaskState> taskStates = new HashMap<>();
        SyncEventProtos.SyncEvent actualMapToProtoResult = SyncEventToProtoMapper.mapToProto(
                new TaskSyncEvent("12", "23", 1L, MessageTypeEnum.REGULAR, 42L,
                        1L, taskStates));
        assertTrue(actualMapToProtoResult.isInitialized());
        assertEquals(1L, actualMapToProtoResult.getEpochOffset());
        assertEquals(0, actualMapToProtoResult.getTaskStatesCount());
        assertEquals("12", actualMapToProtoResult.getTaskUid());
        assertEquals(1L, actualMapToProtoResult.getMessageTimestamp());
        assertEquals(14, actualMapToProtoResult.getSerializedSize());
        assertEquals(42L, actualMapToProtoResult.getRebalanceGenerationId());
        assertEquals(0, actualMapToProtoResult.getMessageTypeValue());
        assertEquals("23", actualMapToProtoResult.getConsumerId());
    }
}