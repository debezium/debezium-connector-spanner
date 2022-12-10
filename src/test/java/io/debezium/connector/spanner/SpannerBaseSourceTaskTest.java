/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
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
}
