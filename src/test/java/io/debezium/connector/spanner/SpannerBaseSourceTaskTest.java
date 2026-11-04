/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.connector.spanner.processor.SourceRecordUtils;
import io.debezium.pipeline.spi.Offsets;

class SpannerBaseSourceTaskTest {

    @Test
    void testGetInitialOffsets() {
        Offsets<SpannerPartition, SpannerOffsetContext> actualInitialOffsets = new SpannerConnectorTask().getInitialOffsets();
        assertEquals(1, actualInitialOffsets.getOffsets().size());
        assertEquals("Parent0", actualInitialOffsets.getTheOnlyPartition().getValue());
    }

    @Test
    void testGetInitialOffsetsInit() {
        SpannerConnectorTask spannerConnectorTask = new SpannerConnectorTask();
        spannerConnectorTask.initialize(mock(SourceTaskContext.class));
        Offsets<SpannerPartition, SpannerOffsetContext> actualInitialOffsets = spannerConnectorTask.getInitialOffsets();
        assertEquals(1, actualInitialOffsets.getOffsets().size());
        assertEquals("Parent0", actualInitialOffsets.getTheOnlyPartition().getValue());
    }

    @Test
    void testGetAllConfigurationFields() {
        assertEquals(SpannerConnectorConfig.ALL_FIELDS, new SpannerConnectorTask().getAllConfigurationFields());
    }

    @Test
    void testVersion() {
        assertNotNull(new SpannerConnectorTask().version());
    }

    @Test
    void testCommitRecordDoesNotChangeTheRecordHeaders() throws InterruptedException {
        SpannerConnectorTask task = spy(new SpannerConnectorTask());
        doNothing().when(task).onRecordSent(any());
        SourceRecord record = new SourceRecord(Map.of(), Map.of(), "Topic", null, Schema.STRING_SCHEMA, "Key",
                Schema.STRING_SCHEMA, "Value", null, SourceRecordUtils.from("1234"));
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("Topic", 0), 0, 0, 0, 0, 0);

        task.commitRecord(record, metadata);

        ArgumentCaptor<SourceRecord> sentRecord = ArgumentCaptor.forClass(SourceRecord.class);
        verify(task).onRecordSent(sentRecord.capture());
        assertNotNull(SourceRecordUtils.extractPublishTimestamp(sentRecord.getValue()));
        assertNull(SourceRecordUtils.extractPublishTimestamp(record));
        assertEquals(1, record.headers().size());
    }
}
