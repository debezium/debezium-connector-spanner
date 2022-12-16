/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.metrics.event.NewQueueMetricEvent;

class MetricsEventPublisherTest {

    @Test
    void testPublishMetricEvent() {
        MetricsEventPublisher metricsEventPublisher = spy(new MetricsEventPublisher());
        Consumer<NewQueueMetricEvent> consumer = mock(Consumer.class);
        metricsEventPublisher.subscribe(NewQueueMetricEvent.class, consumer);
        metricsEventPublisher.publishMetricEvent(new NewQueueMetricEvent());
        verify(consumer).accept(any());
    }

    @Test
    void testSubscribe() {
        MetricsEventPublisher metricsEventPublisher = spy(new MetricsEventPublisher());
        Consumer<NewQueueMetricEvent> consumer = mock(Consumer.class);
        metricsEventPublisher.subscribe(NewQueueMetricEvent.class, consumer);

        assertThrows(IllegalStateException.class,
                () -> metricsEventPublisher.subscribe(NewQueueMetricEvent.class, consumer));
    }

    @Test
    void testLogLatency() {
        MetricsEventPublisher metricsEventPublisher = spy(new MetricsEventPublisher());
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();

        Struct struct = mock(Struct.class);
        Schema schema = mock(Schema.class);
        Field field = mock(Field.class);
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);
        Header headerUid = mock(Header.class);

        SourceRecord sourceRecord = spy(new SourceRecord(sourcePartition, sourceOffset, "Topic",
                new ConnectSchema(Schema.Type.INT8), "Value"));

        doReturn(struct).when(sourceRecord).value();
        doReturn(headers).when(sourceRecord).headers();
        doReturn(header).when(headers).lastWithName(anyString());
        doReturn(1001L).when(header).value();
        doReturn(schema).when(struct).schema();
        doReturn(field).when(schema).field(anyString());
        doReturn(struct).when(struct).getStruct(anyString());

        doReturn(headerUid).when(headers).lastWithName("spannerDataChangeRecordUid");
        doReturn("spannerDataChangeRecordUid").when(headerUid).value();

        metricsEventPublisher.logLatency(sourceRecord);
        verify(metricsEventPublisher, times(1)).publishMetricEvent(any());
    }

    @Test
    void testLogLatencySourceRecordNull() {
        MetricsEventPublisher metricsEventPublisher = spy(new MetricsEventPublisher());
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        SourceRecord sourceRecord = new SourceRecord(sourcePartition, sourceOffset, "Topic",
                new ConnectSchema(Schema.Type.INT8), "Value");
        metricsEventPublisher.logLatency(sourceRecord);
        verify(metricsEventPublisher, times(0)).publishMetricEvent(any());
    }

    @Test
    void testLogLatencySourceRecord() {
        MetricsEventPublisher metricsEventPublisher = spy(new MetricsEventPublisher());
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        ConnectSchema schema = new ConnectSchema(Schema.Type.INT8);
        SourceRecord sourceRecord = spy(new SourceRecord(sourcePartition, sourceOffset, "Topic", schema, "Value"));
        Headers headers = mock(Headers.class);
        doReturn(headers).when(sourceRecord).headers();
        metricsEventPublisher.logLatency(sourceRecord);
        verify(metricsEventPublisher, times(0)).publishMetricEvent(any());
    }

    @Test
    void testNotLogLatency() {
        MetricsEventPublisher metricsEventPublisher = spy(new MetricsEventPublisher());
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();

        SourceRecord sourceRecord = spy(new SourceRecord(sourcePartition, sourceOffset, "Topic",
                new ConnectSchema(Schema.Type.INT8), "Value"));
        Headers headers = mock(Headers.class);
        when(sourceRecord.headers()).thenReturn(headers);
        when(headers.lastWithName(anyString())).thenReturn(null);

        metricsEventPublisher.logLatency(sourceRecord);

        verify(metricsEventPublisher, times(0)).publishMetricEvent(any());
    }
}
