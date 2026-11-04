/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class SourceRecordUtilsTest {

    @Test
    void testExtractRecordUid() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        assertNull(SourceRecordUtils.extractRecordUid(
                new SourceRecord(sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value")));
    }

    @Test
    void testIsDataChangeRecord() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        assertFalse(SourceRecordUtils.isDataChangeRecord(
                new SourceRecord(sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value")));
    }

    @Test
    void testFrom() {
        assertFalse(SourceRecordUtils.from("1234").isEmpty());
    }

    @Test
    void testAddEmitTimestamp() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        assertFalse(SourceRecordUtils.addEmitTimestamp(
                new SourceRecord(sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value"), 10L)
                .headers().isEmpty());
    }

    @Test
    void testAddPublishTimestamp() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        assertFalse(
                SourceRecordUtils
                        .addPublishTimestamp(new SourceRecord(sourcePartition, sourceOffset, "Topic",
                                new ConnectSchema(Schema.Type.INT8), "Value"), 10L)
                        .headers()
                        .isEmpty());
    }

    @Test
    void testAddPollTimestamp() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        assertFalse(
                SourceRecordUtils
                        .addPollTimestamp(new SourceRecord(sourcePartition, sourceOffset, "Topic",
                                new ConnectSchema(Schema.Type.INT8), "Value"), 10L)
                        .headers()
                        .isEmpty());
    }

    @Test
    void testExtractToken() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        assertNull(SourceRecordUtils.extractToken(
                new SourceRecord(sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value")));
    }

    @Test
    void testExtractPublishTimestamp() {
        ConnectHeaders fromResult = SourceRecordUtils.from("1234");
        fromResult.addLong("publishAtTimestamp", 42L);
        SourceRecord sourceRecord = mock(SourceRecord.class);
        when(sourceRecord.headers()).thenReturn(fromResult);
        assertEquals(42L, SourceRecordUtils.extractPublishTimestamp(sourceRecord).longValue());
    }

    @Test
    void testExtractPollTimestamp() {
        ConnectHeaders fromResult = SourceRecordUtils.from("1234");
        fromResult.addLong("pollAtTimestamp", 42L);
        SourceRecord sourceRecord = mock(SourceRecord.class);
        when(sourceRecord.headers()).thenReturn(fromResult);
        assertEquals(42L, SourceRecordUtils.extractPollTimestamp(sourceRecord).longValue());
    }

    @Test
    void testAddTimestampsDoNotChangeTheOriginalRecord() {
        Map<String, Object> sourcePartition = Map.of("partitionToken", "token");
        Map<String, Object> sourceOffset = Map.of("offset", 1L);
        SourceRecord original = new SourceRecord(sourcePartition, sourceOffset, "Topic", 0, Schema.STRING_SCHEMA, "Key",
                Schema.STRING_SCHEMA, "Value", 123L, SourceRecordUtils.from("1234"));

        SourceRecord withEmit = SourceRecordUtils.addEmitTimestamp(original, 1L);
        SourceRecord withPoll = SourceRecordUtils.addPollTimestamp(withEmit, 2L);
        SourceRecord withPublish = SourceRecordUtils.addPublishTimestamp(withPoll, 3L);

        assertEquals(1, original.headers().size());
        assertEquals(2, withEmit.headers().size());
        assertEquals(3, withPoll.headers().size());
        assertEquals(1L, SourceRecordUtils.extractEmitTimestamp(withPublish).longValue());
        assertEquals(2L, SourceRecordUtils.extractPollTimestamp(withPublish).longValue());
        assertEquals(3L, SourceRecordUtils.extractPublishTimestamp(withPublish).longValue());
        assertNull(SourceRecordUtils.extractPublishTimestamp(withPoll));

        assertSame(sourcePartition, withPublish.sourcePartition());
        assertSame(sourceOffset, withPublish.sourceOffset());
        assertEquals("Topic", withPublish.topic());
        assertEquals(0, withPublish.kafkaPartition().intValue());
        assertEquals("Key", withPublish.key());
        assertEquals("Value", withPublish.value());
        assertEquals(123L, withPublish.timestamp().longValue());
    }

    @Test
    void testAddTimestampDoesNotChangeHeadersSharedWithTombstone() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        SourceRecord record = new SourceRecord(sourcePartition, sourceOffset, "Topic", null, Schema.STRING_SCHEMA, "Key",
                Schema.STRING_SCHEMA, "Value", null, SourceRecordUtils.from("1234"));
        // EventDispatcher creates the tombstone with the headers of the delete event
        SourceRecord tombstone = record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                null, null, record.timestamp(), record.headers());

        SourceRecordUtils.addEmitTimestamp(tombstone, 1L);

        assertEquals(1, record.headers().size());
        assertNull(SourceRecordUtils.extractEmitTimestamp(record));
    }
}
