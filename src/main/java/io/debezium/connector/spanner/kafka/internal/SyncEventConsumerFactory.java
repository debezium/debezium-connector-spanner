/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import io.debezium.connector.spanner.SpannerConnectorConfig;

/**
 * Creates Kafka consumer of the Sync topic,
 * based on configuration.
 */
public class SyncEventConsumerFactory<K, V> {

    private final SpannerConnectorConfig config;
    private final boolean autoCommitEnabled;

    public SyncEventConsumerFactory(SpannerConnectorConfig config, boolean autoCommitEnabled) {
        this.config = config;
        this.autoCommitEnabled = autoCommitEnabled;
    }

    public SpannerConnectorConfig getConfig() {
        return config;
    }

    public boolean isAutoCommitEnabled() {
        return autoCommitEnabled;
    }

    public Consumer<K, V> createConsumer(String consumerGroup) {

        final Properties props = config.kafkaProps(
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommitEnabled));

        return new KafkaConsumer<>(props);
    }
}
