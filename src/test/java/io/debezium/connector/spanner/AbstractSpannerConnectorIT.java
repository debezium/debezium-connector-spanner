/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.config.BaseSpannerConnectorConfig;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.connector.spanner.util.KafkaEnvironment;
import io.debezium.connector.spanner.util.PartitionMode;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Base class for Spanner connector integration tests.
 *
 * <p>Subclasses are emulator-only by default: {@link #databaseConnection}, {@link #baseConfig} and
 * {@link #basePgConfig} always point at the local Spanner emulator Docker container started by the
 * build. Tests that are annotated with {@link RealSpannerCompatible} can opt into a real Cloud Spanner
 * backend by overriding those fields (see {@link MutableKeyRangeIT} for an example) and using
 * {@link RealSpannerTestSupport}. That way a single {@code mvn verify -Dspanner.test.real=true ...}
 * command can run the full suite, with the old tests on the emulator and the annotated tests on real
 * Spanner.
 */
public class AbstractSpannerConnectorIT extends AbstractAsyncEngineConnectorTest {

    private static final KafkaEnvironment KAFKA_ENVIRONMENT = new KafkaEnvironment(
            KafkaEnvironment.DOCKER_COMPOSE_FILE);
    protected static final Database database = Database.TEST_DATABASE;
    protected static final Connection databaseConnection = database.getConnection();
    protected static final Database pgDatabase = Database.TEST_PG_DATABASE;
    protected static final Connection pgDatabaseConnection = pgDatabase.getConnection();
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";

    static {
        if (!KAFKA_ENVIRONMENT.isStarted()) {
            Testing.Print.enable();
            KAFKA_ENVIRONMENT.start();
            KAFKA_ENVIRONMENT.setStarted();
        }
    }

    protected static final Configuration baseConfig = createBaseConfigBuilder(database, false).build();
    protected static final Configuration basePgConfig = Configuration.copy(baseConfig)
            .with("gcp.spanner.instance.id", pgDatabase.getInstanceId())
            .with("gcp.spanner.project.id", pgDatabase.getProjectId())
            .with("gcp.spanner.database.id", pgDatabase.getDatabaseId())
            .build();

    /**
     * Builds a connector {@link Configuration} for the given {@link Database}. {@code realSpanner}
     * controls whether the config points at a real Cloud Spanner instance (using the credentials and
     * endpoint passed via {@code -D} system properties) or at the local emulator.
     */
    protected static Configuration.Builder createBaseConfigBuilder(Database database, boolean realSpanner) {
        Configuration.Builder builder = Configuration.create()
                .with("gcp.spanner.instance.id", database.getInstanceId())
                .with("gcp.spanner.project.id", database.getProjectId())
                .with("gcp.spanner.database.id", database.getDatabaseId())
                .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with("connector.spanner.sync.kafka.bootstrap.servers", KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress())
                .with("internal.schema.history.kafka.bootstrap.servers", KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress())
                .with("bootstrap.servers", KAFKA_ENVIRONMENT.kafkaBrokerApiOn().getAddress())
                .with("heartbeat.interval.ms", "300000")
                .with("gcp.spanner.low-watermark.enabled", false)
                .with("tasks.max", 3); // see DBZ-8428
        if (realSpanner) {
            if (System.getProperty("gcp.spanner.host") != null) {
                builder.with("gcp.spanner.host", System.getProperty("gcp.spanner.host"));
            }
            if (System.getProperty("gcp.spanner.credentials.path") != null) {
                builder.with("gcp.spanner.credentials.path", System.getProperty("gcp.spanner.credentials.path"));
            }
            if (System.getProperty("gcp.spanner.credentials.json") != null) {
                builder.with("gcp.spanner.credentials.json", System.getProperty("gcp.spanner.credentials.json"));
            }
        }
        else {
            builder.with("gcp.spanner.emulator.host", "http://localhost:9010");
        }
        if (System.getProperty(BaseSpannerConnectorConfig.SPANNER_TYPE_PROPERTY_NAME) != null) {
            builder.with(BaseSpannerConnectorConfig.SPANNER_TYPE_PROPERTY_NAME, System.getProperty(BaseSpannerConnectorConfig.SPANNER_TYPE_PROPERTY_NAME));
        }
        if (!realSpanner && System.getProperty("gcp.spanner.host") != null) {
            builder.with("gcp.spanner.host", System.getProperty("gcp.spanner.host"));
        }
        if (System.getProperty("spanner.omni.use.plaintext") != null) {
            builder.with("spanner.omni.use.plaintext", System.getProperty("spanner.omni.use.plaintext"));
        }
        if (System.getProperty("spanner.omni.client.key.path") != null && System.getProperty("spanner.omni.client.cert.path") != null) {
            builder.with("spanner.omni.client.key.path", System.getProperty("spanner.omni.client.key.path"));
            builder.with("spanner.omni.client.cert.path", System.getProperty("spanner.omni.client.cert.path"));
        }
        return builder;
    }

    protected static Configuration buildTestConfig(Configuration baseConfig, String changeStreamName,
                                                   String tableName, PartitionMode partitionMode) {
        Configuration.Builder configBuilder = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
        if (partitionMode == PartitionMode.MUTABLE_KEY_RANGE) {
            configBuilder.with("gcp.spanner.mutable.window.minutes", 1);
        }
        return configBuilder.build();
    }

    @BeforeAll
    public static void before() throws InterruptedException {
        Testing.Print.enable();
    }

    @AfterAll
    public static void after() throws InterruptedException {
        Testing.print("Cleaning up kafka...");
        KAFKA_ENVIRONMENT.clearTopics();
        Testing.print("Cleaning complete!");
    }

    protected static void clearKafkaTopics() {
        KAFKA_ENVIRONMENT.clearTopics();
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "30"));
    }

    protected String getTopicName(Configuration config, String tableName) {
        String debeziumConnectorName = "testing-connector";
        return debeziumConnectorName + "." + tableName;
    }
}
