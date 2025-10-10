/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerConnectorTask;
import io.debezium.connector.spanner.context.source.SpannerSourceTaskContext;
import io.debezium.connector.spanner.metrics.jmx.SpannerSnapshotChangeEventSourceMetricsStub;
import io.debezium.connector.spanner.metrics.jmx.SpannerStreamingChangeEventSourceMetrics;
import io.debezium.connector.spanner.processor.metadata.SpannerEventMetadataProvider;

class SpannerChangeEventSourceMetricsFactoryTest {

    @Test
    void testGetSnapshotMetrics() {
        SpannerChangeEventSourceMetricsFactory factory = new SpannerChangeEventSourceMetricsFactory(null);
        assertTrue(factory.getSnapshotMetrics(null, null, null) instanceof SpannerSnapshotChangeEventSourceMetricsStub);
    }

    @Test
    void testGetStreamingMetrics() {
        SpannerConnectorConfig connectorConfig = Mockito.mock(SpannerConnectorConfig.class);

        SpannerChangeEventSourceMetricsFactory spannerChangeEventSourceMetricsFactory = new SpannerChangeEventSourceMetricsFactory(
                new SpannerMeter(new SpannerConnectorTask(), connectorConfig, null, () -> null));
        SpannerSourceTaskContext spannerSourceTaskContext = mock(SpannerSourceTaskContext.class);
        when(spannerSourceTaskContext.getConnectorType()).thenReturn("Connector Type");
        when(spannerSourceTaskContext.getConnectorLogicalName()).thenReturn("Connector Name");
        when(spannerSourceTaskContext.getTaskId()).thenReturn("42");

        SpannerStreamingChangeEventSourceMetrics streamingMetrics = (SpannerStreamingChangeEventSourceMetrics) spannerChangeEventSourceMetricsFactory.getStreamingMetrics(
                spannerSourceTaskContext, null, new SpannerEventMetadataProvider(), Collections::emptyList);
        assertNull(streamingMetrics.getCapturedTables());
        assertNull(streamingMetrics.getLastTransactionId());
        assertNull(streamingMetrics.getLastEvent());
        assertEquals(0, streamingMetrics.getErrorCount());
    }
}
