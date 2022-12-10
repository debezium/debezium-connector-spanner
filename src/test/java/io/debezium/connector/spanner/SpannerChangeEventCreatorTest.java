/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashMap;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

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
        assertSame(sourceRecord, dataChangeEvent.getRecord());
    }
}
