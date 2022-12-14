/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import io.debezium.connector.spanner.SpannerConnectorConfig;

/**
 * Creates Kafka producer, based on configuration
 */
public class ProducerFactory<K, V> {

    private final SpannerConnectorConfig config;

    public ProducerFactory(SpannerConnectorConfig config) {
        this.config = config;
    }

    public KafkaProducer<K, V> createProducer() {
        Properties properties = config.kafkaProps(
                Map.of(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                        ProducerConfig.ACKS_CONFIG, "1",
                        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(config.syncRequestTimeout()),
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(config.syncDeliveryTimeout())));

        return new KafkaProducer<>(properties);
    }
}
