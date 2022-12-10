/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor.heartbeat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.function.BlockingConsumer;
import io.debezium.util.SchemaNameAdjuster;

class SpannerHeartbeatTest {

    @Test
    void testConstructor() {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust(any())).thenReturn("Adjust");
        try (SpannerHeartbeat spannerHeartbeat = new SpannerHeartbeat("Topic Name", schemaNameAdjuster)) {
            assertTrue(spannerHeartbeat.isEnabled());
        }

        verify(schemaNameAdjuster, atLeast(1)).adjust(any());
    }

    @Test
    void testConstructorThrows() {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust(any())).thenThrow(new IllegalStateException());
        assertThrows(IllegalStateException.class, () -> new SpannerHeartbeat("Topic Name", schemaNameAdjuster));

        verify(schemaNameAdjuster).adjust(any());
    }

    @Test
    void testHeartbeatThrows() {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust(any())).thenReturn("Adjust");
        try (SpannerHeartbeat spannerHeartbeat = new SpannerHeartbeat("Topic Name", schemaNameAdjuster)) {
            HashMap<String, Object> partition = new HashMap<>();
            assertThrows(IllegalStateException.class, () -> spannerHeartbeat.heartbeat(partition, new HashMap<>(),
                    (BlockingConsumer<SourceRecord>) mock(BlockingConsumer.class)));
        }
        verify(schemaNameAdjuster, atLeast(1)).adjust(any());
    }

    @Test
    void testForcedBeat() throws InterruptedException {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust(any())).thenReturn("Adjust");
        try (SpannerHeartbeat spannerHeartbeat = spy(new SpannerHeartbeat("Topic Name", schemaNameAdjuster))) {
            Map<String, Object> partition = new HashMap<>();
            partition.put("partitionToken", "v1");
            Map<String, Object> offset = new HashMap<>();
            offset.put("partitionToken", "v1");
            BlockingConsumer<SourceRecord> consumer = mock(BlockingConsumer.class);
            doNothing().when(consumer).accept(any());
            spannerHeartbeat.heartbeat(partition, offset, consumer);
            verify(consumer).accept(any());
            verify(spannerHeartbeat).forcedBeat(any(), any(), any());
        }
        verify(schemaNameAdjuster, atLeast(1)).adjust(any());
    }

    @Test
    void testForcedBeatThrow() {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust(any())).thenReturn("Adjust");
        try (SpannerHeartbeat spannerHeartbeat = new SpannerHeartbeat("Topic Name", schemaNameAdjuster)) {
            HashMap<String, Object> partition = new HashMap<>();
            assertThrows(IllegalStateException.class, () -> spannerHeartbeat.forcedBeat(partition, new HashMap<>(),
                    (BlockingConsumer<SourceRecord>) mock(BlockingConsumer.class)));
        }
        verify(schemaNameAdjuster, atLeast(1)).adjust(any());
    }

    @Test
    void testPartitionTokenKey() {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust(any())).thenReturn("Adjust");
        SpannerHeartbeat spannerHeartbeat = new SpannerHeartbeat("Topic Name", schemaNameAdjuster);
        String value = "value";
        Struct struct = spannerHeartbeat.partitionTokenKey(value);
        assertEquals(value, struct.get("partitionToken"));
    }

    @Test
    void testMessageValue() {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust(any())).thenReturn("Adjust");
        SpannerHeartbeat spannerHeartbeat = new SpannerHeartbeat("Topic Name", schemaNameAdjuster);
        Struct struct = spannerHeartbeat.messageValue();
        assertNotNull(struct.get("ts_ms"));
    }

    @Test
    void testHeartbeatRecord() {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust(any())).thenReturn("Adjust");
        SpannerHeartbeat spannerHeartbeat = new SpannerHeartbeat("Topic Name", schemaNameAdjuster);
        assertNotNull(spannerHeartbeat.heartbeatRecord(Map.of("partitionToken", "v"), Map.of("partitionToken", "v")));
    }
}
