/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import java.util.Properties;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.testcontainers.containers.ContainerState;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class KafkaBrokerApi<K, V> {

    protected static final String SCHEMA_REGISTRY_PORT = "8081";

    protected static final String SCHEMA_REGISTRY_HOST = "http://localhost";

    protected static final int POLL_DURATION_MILLIS = 100;

    protected static final int WAIT_TOPIC_HAS_NO_MORE_RECORDS_SECONDS = 60;

    public static final int POLL_FIRST_RECORDS_TIMEOUT_MAX_MINUTES = 10;

    private final ContainerState containerState;

    private final int kafkaPort;

    private final Properties properties;

    public KafkaBrokerApi(ContainerState containerState, int kafkaPort, Properties properties) {
        this.containerState = containerState;
        this.kafkaPort = kafkaPort;
        this.properties = SerializationUtils.clone(properties);
    }

    public static String getSchemaRegistryAddress() {
        return SCHEMA_REGISTRY_HOST + ":" + SCHEMA_REGISTRY_PORT;
    }

    public static KafkaBrokerApi<ObjectNode, ObjectNode> createKafkaBrokerApiObjectNode(ContainerState containerState,
                                                                                        int kafkaPort) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, containerState.getHost() + ":" + kafkaPort);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaBrokerApi<>(containerState, kafkaPort, props);
    }

    public String getAddress() {
        return containerState.getHost() + ":" + kafkaPort;
    }

    public AdminClient createAdminClient() {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getAddress());
        return AdminClient.create(props);
    }
}
