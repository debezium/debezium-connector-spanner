/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;

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
}
