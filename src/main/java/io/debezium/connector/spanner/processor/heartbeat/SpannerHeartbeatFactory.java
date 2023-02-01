/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor.heartbeat;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Creates {@link SpannerHeartbeat} based on configured properties
 *
 */
public class SpannerHeartbeatFactory extends HeartbeatFactory<TableId> {

    private final TopicNamingStrategy topicNamingStrategy;
    private final SchemaNameAdjuster schemaNameAdjuster;

    public SpannerHeartbeatFactory(CommonConnectorConfig connectorConfig, TopicNamingStrategy topicNamingStrategy, SchemaNameAdjuster schemaNameAdjuster) {
        super(connectorConfig, topicNamingStrategy, schemaNameAdjuster);

        this.topicNamingStrategy = topicNamingStrategy;
        this.schemaNameAdjuster = schemaNameAdjuster;
    }

    public Heartbeat createHeartbeat() {
        return new SpannerHeartbeat(topicNamingStrategy.heartbeatTopic(), schemaNameAdjuster);
    }

}
