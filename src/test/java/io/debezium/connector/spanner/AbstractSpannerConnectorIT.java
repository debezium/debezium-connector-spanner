/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import org.junit.jupiter.api.AfterAll;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.connector.spanner.util.KafkaEnvironment;
import io.debezium.embedded.AbstractConnectorTest;

public class AbstractSpannerConnectorIT extends AbstractConnectorTest {

    private static final KafkaEnvironment KAFKA_ENVIRONMENT = new KafkaEnvironment(
            KafkaEnvironment.DOCKER_COMPOSE_FILE);
    protected static final Database database = Database.TEST_DATABASE;
    protected static final Connection databaseConnection = database.getConnection();
    protected static final int WAIT_FOR_CDC = 1000;

    static {
        if (!KAFKA_ENVIRONMENT.isStarted()) {
            KAFKA_ENVIRONMENT.start();
            KAFKA_ENVIRONMENT.setStarted();
        }
    }

    protected static final Configuration baseConfig = Configuration.create()
            .with("gcp.spanner.instance.id", database.getInstanceId())
            .with("gcp.spanner.project.id", database.getProjectId())
            .with("gcp.spanner.database.id", database.getDatabaseId())
            .with("gcp.spanner.emulator.host",
                    "http://localhost:9010")
            .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            .with("connector.spanner.sync.kafka.bootstrap.servers", KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress())
            .with("database.history.kafka.bootstrap.servers", KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress())
            .with("bootstrap.servers", KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress())
            .with("heartbeat.interval.ms", "300000")
            .with("gcp.spanner.low-watermark.enabled", false)
            .build();

    @AfterAll
    public static void after() throws InterruptedException {
        System.out.println("Cleaning up kafka...");
        KAFKA_ENVIRONMENT.clearTopics();
        System.out.println("Cleaning complete!");
    }

    protected static void waitForCDC() {
        try {
            Thread.sleep(WAIT_FOR_CDC);
        }
        catch (Exception e) {

        }
    }

    protected String getTopicName(Configuration config, String tableName) {
        String debeziumConnectorName = "testing-connector";
        return debeziumConnectorName + "." + tableName;
    }
}
