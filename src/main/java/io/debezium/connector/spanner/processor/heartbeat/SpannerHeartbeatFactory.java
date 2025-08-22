/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor.heartbeat;

import static io.debezium.config.CommonConnectorConfig.TOPIC_NAMING_STRATEGY;

import java.time.Clock;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.heartbeat.DebeziumHeartbeatFactory;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatConnectionProvider;
import io.debezium.heartbeat.HeartbeatErrorHandler;
import io.debezium.pipeline.DataChangeEvent;

/**
 * Creates {@link SpannerHeartbeat} based on configured properties
 *
 */
public class SpannerHeartbeatFactory implements DebeziumHeartbeatFactory {

    @Override
    public Heartbeat.ScheduledHeartbeat getScheduledHeartbeat(CommonConnectorConfig connectorConfig,
                                                              HeartbeatConnectionProvider connectionProvider,
                                                              HeartbeatErrorHandler errorHandler,
                                                              ChangeEventQueue<DataChangeEvent> queue) {
        return new SpannerHeartbeat(
                connectorConfig.getTopicNamingStrategy(TOPIC_NAMING_STRATEGY).heartbeatTopic(),
                connectorConfig.schemaNameAdjuster(),
                queue, Clock.systemUTC());
    }
}
