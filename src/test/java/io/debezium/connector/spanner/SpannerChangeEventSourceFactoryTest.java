/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.spanner.metrics.SpannerMeter;
import io.debezium.connector.spanner.task.SynchronizedPartitionManager;

class SpannerChangeEventSourceFactoryTest {

    @Test
    void testGetSnapshotChangeEventSource() {
        SpannerChangeEventSourceFactory spannerChangeEventSourceFactory = new SpannerChangeEventSourceFactory(
                null, null, null, null, null, null, null, null);

        assertNotNull(spannerChangeEventSourceFactory.getSnapshotChangeEventSource(null, null));
    }

    @Test
    void testGetStreamingChangeEventSource() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(configuration);
        SpannerMeter spannerMeter = new SpannerMeter(new SpannerConnectorTask(), connectorConfig, null, null);

        SpannerChangeEventSourceFactory spannerChangeEventSourceFactory = new SpannerChangeEventSourceFactory(
                connectorConfig, null, null, null, spannerMeter, null, null, new SynchronizedPartitionManager(null));
        assertNotNull(spannerChangeEventSourceFactory.getStreamingChangeEventSource());
    }
}
