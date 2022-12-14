/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.debezium.connector.spanner.SpannerConnectorConfig;

/**
 * Creates Kafka consumer of the Rebalance topic,
 * based on configuration.
 */
public class RebalancingConsumerFactory<K, V> {

    private final SpannerConnectorConfig config;

    public RebalancingConsumerFactory(SpannerConnectorConfig config) {
        this.config = config;
    }

    public SpannerConnectorConfig getConfig() {
        return config;
    }

    private Consumer<K, V> createConsumer(String consumerGroup) {

        final Properties props = config.kafkaProps(
                Map.of(
                        ConsumerConfig.GROUP_ID_CONFIG, consumerGroup,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false));

        return new KafkaConsumer<>(props);
    }

    public Consumer<K, V> createSubscribeConsumer(String consumerGroup, String topic,
                                                  ConsumerRebalanceListener rebalanceListener) {
        Consumer<K, V> consumer = createConsumer(consumerGroup);
        consumer.subscribe(Collections.singletonList(topic), rebalanceListener);
        return consumer;
    }

}
