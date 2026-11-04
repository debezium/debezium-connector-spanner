/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashMap;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.processor.SourceRecordUtils;
import io.debezium.pipeline.DataChangeEvent;

class SpannerChangeEventCreatorTest {

    @Test
    void testCreateDataChangeEvent() {
        SpannerChangeEventCreator spannerChangeEventCreator = new SpannerChangeEventCreator();
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        SourceRecord sourceRecord = new SourceRecord(sourcePartition, sourceOffset, "Topic",
                new ConnectSchema(Schema.Type.INT8), "Value");

        DataChangeEvent dataChangeEvent = spannerChangeEventCreator.createDataChangeEvent(sourceRecord);
        SourceRecord record = dataChangeEvent.getRecord();
        assertSame(sourcePartition, record.sourcePartition());
        assertSame(sourceOffset, record.sourceOffset());
        assertEquals("Topic", record.topic());
        assertEquals("Value", record.value());
        assertNotNull(SourceRecordUtils.extractEmitTimestamp(record));
        assertNull(SourceRecordUtils.extractEmitTimestamp(sourceRecord));
    }
}
