/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor.heartbeat;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.SchemaNameAdjuster;

class SpannerHeartbeatFactoryTest {

    @Test
    void testCreateHeartbeat() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");

        SpannerHeartbeatFactory spannerHeartbeatFactory = new SpannerHeartbeatFactory(
                new SpannerConnectorConfig(configuration), mock(TopicNamingStrategy.class), mock(SchemaNameAdjuster.class));
        try (Heartbeat heartbeat = spannerHeartbeatFactory.createHeartbeat()) {
            assertTrue(heartbeat instanceof SpannerHeartbeat);
            assertTrue(heartbeat.isEnabled());
        }
    }
}
