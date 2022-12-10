/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka;

import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;

import io.debezium.connector.spanner.SpannerConnectorConfig;

/**
 * Creates Kafka Admin Client based on configuration.
 */
public class KafkaAdminClientFactory implements AutoCloseable {

    private volatile AdminClient adminClient;
    private final SpannerConnectorConfig connectorConfig;

    public KafkaAdminClientFactory(SpannerConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public synchronized AdminClient getAdminClient() {
        if (adminClient == null) {
            adminClient = createAdminClient();
        }
        return adminClient;
    }

    private AdminClient createAdminClient() {
        return AdminClient.create(connectorConfig.kafkaProps(Map.of()));
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
            adminClient = null;
        }
    }
}
