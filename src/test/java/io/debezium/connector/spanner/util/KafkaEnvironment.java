/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.util.Testing;

public class KafkaEnvironment {

    private static final String KAFKA_BROKER_SERVICE_NAME = "broker_1";
    private static final int KAFKA_BROKER_SERVICE_API_PORT = 9092;

    public static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(200L);
    public static final Duration STARTUP_CONNECTOR_TIMEOUT = Duration.ofSeconds(600L);
    public static final Duration CONFIGURE_CONNECTOR_TIMEOUT = Duration.ofSeconds(200L);

    public static final String DOCKER_COMPOSE_FILE = "src/test/java/io/debezium/connector/spanner/util/docker-compose.yml";
    public static final KafkaEnvironment TEST_KAFKA_ENVIRONMENT = new KafkaEnvironment(DOCKER_COMPOSE_FILE);

    private boolean isStarted = false;

    private DockerComposeContainer composeContainer;

    private KafkaBrokerApi<ObjectNode, ObjectNode> brokerApiOn;

    private KafkaBrokerApi<GenericRecord, GenericRecord> brokerApiGr;

    public KafkaEnvironment(String dockerComposeFilePath) {
        Testing.Print.enable();
        Testing.print("Initializing kafka environment for IT test...");
        this.composeContainer = new DockerComposeContainer(new File(dockerComposeFilePath))
                .withExposedService(KAFKA_BROKER_SERVICE_NAME, KAFKA_BROKER_SERVICE_API_PORT,
                        Wait.forListeningPort().withStartupTimeout(STARTUP_TIMEOUT));
        Testing.print("Finished initializing kafka environment.");
    }

    public void start() {

        Testing.print("Starting Kafka environment");
        this.composeContainer.start();
        ContainerState brokerState = (ContainerState) composeContainer
                .getContainerByServiceName(KAFKA_BROKER_SERVICE_NAME)
                .orElseThrow();

        this.brokerApiOn = KafkaBrokerApi.createKafkaBrokerApiObjectNode(brokerState, KAFKA_BROKER_SERVICE_API_PORT);
    }

    public KafkaBrokerApi<ObjectNode, ObjectNode> kafkaBrokerApiOn() {
        return brokerApiOn;
    }

    public boolean isStarted() {
        return isStarted;
    }

    public void setStarted() {
        isStarted = true;
    }

    public void clearTopics() {
        try (AdminClient adminClient = kafkaBrokerApiOn().createAdminClient()) {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            Set<String> topics = listTopicsResult.names().get();
            Arrays.asList("_kafka-connect-configs",
                    "_kafka-connect-offsets",
                    "_kafka-connect-status",
                    "_kafka-connect-status",
                    "_schemas",
                    "_confluent-command",
                    "_confluent_balancer_api_state",
                    "_confluent-metrics",
                    "__consumer_offsets",
                    "_confluent-telemetry-metrics",
                    "_rebalancing_topic_spanner_connector_testing-connector").forEach(
                            topics::remove);
            adminClient.deleteTopics(topics);
        }
        catch (Exception e) {
            Testing.print(e.getMessage());
        }
    }
}
