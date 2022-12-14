/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

class SpannerEventMetadataProviderTest {

    @Test
    void testGetEventTimestampNull() {
        SpannerEventMetadataProvider spannerEventMetadataProvider = new SpannerEventMetadataProvider();
        assertNull(spannerEventMetadataProvider.getEventTimestamp(
                new TableId("Catalog Name", "Schema Name", "Table Name"), mock(SpannerOffsetContext.class), "Key", null));
    }

    @Test
    void testGetEventTimestampNullValue() {
        SpannerEventMetadataProvider spannerEventMetadataProvider = new SpannerEventMetadataProvider();
        TableId tableId = new TableId("Catalog Name", "Schema Name", "Table Name");
        assertNull(spannerEventMetadataProvider.getEventTimestamp(
                tableId, mock(SpannerOffsetContext.class), "Key", null));
    }

    @Test
    void testGetEventTimestampTableIdNull() {
        SpannerEventMetadataProvider spannerEventMetadataProvider = new SpannerEventMetadataProvider();
        Struct struct = mock(Struct.class);
        assertNull(spannerEventMetadataProvider.getEventTimestamp(
                null, mock(SpannerOffsetContext.class), "Key", struct));
    }

    @Test
    void testGetEventTimestamp() {
        SpannerEventMetadataProvider spannerEventMetadataProvider = new SpannerEventMetadataProvider();
        Struct struct = mock(Struct.class);
        TableId tableId = new TableId("Catalog Name", "Schema Name", "Table Name");

        when(struct.getStruct(anyString())).thenReturn(struct);
        when(struct.getInt64(anyString())).thenReturn(1L);

        assertNotNull(spannerEventMetadataProvider.getEventTimestamp(
                tableId, mock(SpannerOffsetContext.class), "Key", struct));

        when(struct.getInt64(anyString())).thenReturn(null);
        assertNull(spannerEventMetadataProvider.getEventTimestamp(
                tableId, mock(SpannerOffsetContext.class), "Key", struct));
    }

    @Test
    void testGetEventSourcePositionNullValue() {
        SpannerEventMetadataProvider spannerEventMetadataProvider = new SpannerEventMetadataProvider();
        TableId tableId = new TableId("Catalog Name", "Schema Name", "Table Name");
        assertNull(spannerEventMetadataProvider.getEventSourcePosition(
                tableId, mock(SpannerOffsetContext.class), "Key", null));
    }

    @Test
    void testGetEventSourcePositionTableIdNull() {
        SpannerEventMetadataProvider spannerEventMetadataProvider = new SpannerEventMetadataProvider();
        Struct struct = mock(Struct.class);
        assertNull(spannerEventMetadataProvider.getEventSourcePosition(
                null, mock(SpannerOffsetContext.class), "Key", struct));
    }

    @Test
    void testGetEventSourcePositionTable() {
        SpannerEventMetadataProvider spannerEventMetadataProvider = new SpannerEventMetadataProvider();
        Struct struct = mock(Struct.class);
        TableId tableId = new TableId("Catalog Name", "Schema Name", "Table Name");

        when(struct.getStruct(anyString())).thenReturn(struct);

        assertNotNull(spannerEventMetadataProvider.getEventSourcePosition(
                tableId, mock(SpannerOffsetContext.class), "Key", struct));
    }

    @Test
    void testGetTransactionIdNullValue() {
        SpannerEventMetadataProvider spannerEventMetadataProvider = new SpannerEventMetadataProvider();
        TableId tableId = new TableId("Catalog Name", "Schema Name", "Table Name");
        assertNull(spannerEventMetadataProvider.getTransactionId(
                tableId, mock(SpannerOffsetContext.class), "Key", null));
    }

    @Test
    void testGetTransactionIdTableIdNull() {
        SpannerEventMetadataProvider spannerEventMetadataProvider = new SpannerEventMetadataProvider();
        Struct struct = mock(Struct.class);
        assertNull(spannerEventMetadataProvider.getTransactionId(
                null, mock(SpannerOffsetContext.class), "Key", struct));
    }

    @Test
    void testGetTransactionIdTable() {
        SpannerEventMetadataProvider spannerEventMetadataProvider = new SpannerEventMetadataProvider();
        Struct struct = mock(Struct.class);
        TableId tableId = new TableId("Catalog Name", "Schema Name", "Table Name");

        when(struct.getStruct(anyString())).thenReturn(struct);
        when(struct.getString(anyString())).thenReturn("struct");

        assertNotNull(spannerEventMetadataProvider.getTransactionId(
                tableId, mock(SpannerOffsetContext.class), "Key", struct));
    }

    private static Stream<Arguments> summaryStringProvider() {
        return Stream.of(
                Arguments.of(null, null, "42", null, "key: 42"),
                Arguments.of(null, null, null, null, ""),
                Arguments.of(new TableId("Catalog Name", "Schema Name", "Table Name"),
                        mock(SpannerOffsetContext.class), "Key", null, "key: Key")

        );
    }

    @ParameterizedTest
    @MethodSource("summaryStringProvider")
    void testToSummaryString(DataCollectionId source, OffsetContext offset, Object key, Struct value, String expected) {
        SpannerEventMetadataProvider spannerEventMetadataProvider = new SpannerEventMetadataProvider();
        assertEquals(expected, spannerEventMetadataProvider.toSummaryString(source, offset, key, value));
    }
}
