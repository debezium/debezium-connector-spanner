/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.latency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class LatencyCalculatorTest {

    @Test
    void testGetTotalLatencyNoSource() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        assertNull(LatencyCalculator.getTotalLatency(
                new SourceRecord(sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value")));
    }

    @Test
    void testGetReadToEmitLatencyNoSource() {
        assertNull(LatencyCalculator.getReadToEmitLatency(
                new SourceRecord(new HashMap<>(), new HashMap<>(), "Topic", new ConnectSchema(Schema.Type.INT8), "Value")));
    }

    @Test
    void testGetSpannerLatencyNoSource() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        assertNull(LatencyCalculator.getSpannerLatency(
                new SourceRecord(sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value")));
    }

    @Test
    void testGetCommitToEmitLatencyNoSource() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        assertNull(LatencyCalculator.getCommitToEmitLatency(
                new SourceRecord(sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value")));
    }

    @Test
    void testGetCommitToPublishLatencyNoSource() {
        SourceRecord sourceRecord = spy(new SourceRecord(
                new HashMap<>(), new HashMap<>(), "Topic", new ConnectSchema(Schema.Type.INT8), "Value"));
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);
        when(header.value()).thenReturn(null);
        when(headers.lastWithName(anyString())).thenReturn(header);
        when(sourceRecord.headers()).thenReturn(headers);

        Long commitToPublishLatency = LatencyCalculator.getCommitToPublishLatency(sourceRecord);
        assertNull(commitToPublishLatency);
    }

    @Test
    void testGetEmitToPublishLatencyNoSource() {
        SourceRecord sourceRecord = spy(new SourceRecord(
                new HashMap<>(), new HashMap<>(), "Topic", new ConnectSchema(Schema.Type.INT8), "Value"));
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);
        when(header.value()).thenReturn(null);
        when(headers.lastWithName(anyString())).thenReturn(header);
        when(sourceRecord.headers()).thenReturn(headers);

        Long emitToPublishLatency = LatencyCalculator.getEmitToPublishLatency(sourceRecord);
        assertNull(emitToPublishLatency);
    }

    @Test
    void testGetOwnConnectorLatencyNoSource() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        assertNull(LatencyCalculator.getOwnConnectorLatency(
                new SourceRecord(sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value")));
    }

    @Test
    void testGetLowWatermarkLagNoSource() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        assertNull(LatencyCalculator.getLowWatermarkLag(
                new SourceRecord(sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value")));
    }

    @Test
    void testGetTotalLatency() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        Struct struct = mock(Struct.class);
        Schema schema = mock(Schema.class);
        Field field = mock(Field.class);
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);

        SourceRecord sourceRecord = spy(new SourceRecord(
                sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value"));

        doReturn(struct).when(sourceRecord).value();
        doReturn(headers).when(sourceRecord).headers();
        doReturn(header).when(headers).lastWithName(anyString());
        doReturn(1L).when(header).value();
        doReturn(schema).when(struct).schema();
        doReturn(field).when(schema).field(anyString());
        doReturn(struct).when(struct).getStruct(anyString());

        assertEquals(1L, LatencyCalculator.getTotalLatency(sourceRecord));
    }

    @Test
    void testGetReadToEmitLatency() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        Struct struct = mock(Struct.class);
        Schema schema = mock(Schema.class);
        Field field = mock(Field.class);
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);

        SourceRecord sourceRecord = spy(new SourceRecord(
                sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value"));

        doReturn(struct).when(sourceRecord).value();
        doReturn(headers).when(sourceRecord).headers();
        doReturn(header).when(headers).lastWithName(anyString());
        doReturn(1L).when(header).value();
        doReturn(schema).when(struct).schema();
        doReturn(field).when(schema).field(anyString());
        doReturn(struct).when(struct).getStruct(anyString());

        assertEquals(1L, LatencyCalculator.getReadToEmitLatency(sourceRecord));
    }

    @Test
    void testGetSpannerLatency() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        Struct struct = mock(Struct.class);
        Schema schema = mock(Schema.class);
        Field field = mock(Field.class);
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);

        SourceRecord sourceRecord = spy(new SourceRecord(
                sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value"));

        doReturn(struct).when(sourceRecord).value();
        doReturn(headers).when(sourceRecord).headers();
        doReturn(header).when(headers).lastWithName(anyString());
        doReturn(1L).when(header).value();
        doReturn(schema).when(struct).schema();
        doReturn(field).when(schema).field(anyString());
        doReturn(struct).when(struct).getStruct(anyString());

        assertEquals(0L, LatencyCalculator.getSpannerLatency(sourceRecord));
    }

    @Test
    void testGetCommitToEmitLatency() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        Struct struct = mock(Struct.class);
        Schema schema = mock(Schema.class);
        Field field = mock(Field.class);
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);

        SourceRecord sourceRecord = spy(new SourceRecord(
                sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value"));

        doReturn(struct).when(sourceRecord).value();
        doReturn(headers).when(sourceRecord).headers();
        doReturn(header).when(headers).lastWithName(anyString());
        doReturn(1L).when(header).value();
        doReturn(schema).when(struct).schema();
        doReturn(field).when(schema).field(anyString());
        doReturn(struct).when(struct).getStruct(anyString());

        assertEquals(1L, LatencyCalculator.getCommitToEmitLatency(sourceRecord));
    }

    @Test
    void testGetCommitToPublishLatency() {
        SourceRecord sourceRecord = spy(new SourceRecord(
                new HashMap<>(), new HashMap<>(), "Topic", new ConnectSchema(Schema.Type.INT8), "Value"));
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);
        when(header.value()).thenReturn(null);
        when(headers.lastWithName(anyString())).thenReturn(header);
        when(sourceRecord.headers()).thenReturn(headers);

        Long commitToPublishLatency = LatencyCalculator.getCommitToPublishLatency(sourceRecord);
        assertNull(commitToPublishLatency);
    }

    @Test
    void testGetEmitToPublishLatency() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        Struct struct = mock(Struct.class);
        Schema schema = mock(Schema.class);
        Field field = mock(Field.class);
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);

        SourceRecord sourceRecord = spy(new SourceRecord(
                sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value"));

        doReturn(struct).when(sourceRecord).value();
        doReturn(headers).when(sourceRecord).headers();
        doReturn(header).when(headers).lastWithName(anyString());
        doReturn(1L).when(header).value();
        doReturn(schema).when(struct).schema();
        doReturn(field).when(schema).field(anyString());
        doReturn(struct).when(struct).getStruct(anyString());

        assertEquals(0L, LatencyCalculator.getEmitToPublishLatency(sourceRecord));
    }

    @Test
    void testGetOwnConnectorLatency() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        Struct struct = mock(Struct.class);
        Schema schema = mock(Schema.class);
        Field field = mock(Field.class);
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);

        SourceRecord sourceRecord = spy(new SourceRecord(
                sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value"));

        doReturn(struct).when(sourceRecord).value();
        doReturn(headers).when(sourceRecord).headers();
        doReturn(header).when(headers).lastWithName(anyString());
        doReturn(1L).when(header).value();
        doReturn(schema).when(struct).schema();
        doReturn(field).when(schema).field(anyString());
        doReturn(struct).when(struct).getStruct(anyString());

        assertEquals(1L, LatencyCalculator.getOwnConnectorLatency(sourceRecord));
    }

    @Test
    void testGetLowWatermarkLag() {
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        Struct struct = mock(Struct.class);
        Schema schema = mock(Schema.class);
        Field field = mock(Field.class);
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);

        SourceRecord sourceRecord = spy(new SourceRecord(
                sourcePartition, sourceOffset, "Topic", new ConnectSchema(Schema.Type.INT8), "Value"));

        doReturn(struct).when(sourceRecord).value();
        doReturn(headers).when(sourceRecord).headers();
        doReturn(header).when(headers).lastWithName(anyString());
        doReturn(1L).when(header).value();
        doReturn(schema).when(struct).schema();
        doReturn(field).when(schema).field(anyString());
        doReturn(struct).when(struct).getStruct(anyString());

        assertNotNull(LatencyCalculator.getLowWatermarkLag(sourceRecord));
    }
}
