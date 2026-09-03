/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;

import com.google.cloud.spanner.Dialect;

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
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";

    static {
        if (!KAFKA_ENVIRONMENT.isStarted()) {
            Testing.Print.enable();
            KAFKA_ENVIRONMENT.start();
            KAFKA_ENVIRONMENT.setStarted();
        }
    }

    protected static final Database database = Database.TEST_DATABASE;
    protected static final Database pgDatabase = Database.TEST_PG_DATABASE;

    /**
     * Override the inherited emulator connection/config with a real-Spanner pair when
     * {@code -Dspanner.test.real=true} is supplied; otherwise keep the parent's emulator pair.
     */
    protected static final Connection databaseConnection = Connection.isRealSpanner()
            ? RealSpannerTestSupport.getConnection(database)
            : database.getConnection();
    protected static final Configuration baseConfig = createBaseConfigBuilder(database, Connection.isRealSpanner()).build();

    /**
     * Same real-Spanner-or-emulator override as {@link #databaseConnection}/{@link #baseConfig}
     * above, but for the PostgreSQL-dialect pair, so tests parameterized over {@link Dialect} get a
     * real PostgreSQL-dialect connection too when {@code -Dspanner.test.real=true} is supplied.
     */
    protected static final Connection pgDatabaseConnection = Connection.isRealSpanner()
            ? RealSpannerTestSupport.getConnection(pgDatabase)
            : pgDatabase.getConnection();
    protected static final Configuration basePgConfig = Connection.isRealSpanner()
            ? createBaseConfigBuilder(pgDatabase, true).build()
            : Configuration.copy(baseConfig)
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

    void createTableAndStream(Connection connection, PartitionMode mode, String table, String stream) {
        try {
            String tableParams = "(id INT64, value STRING(100)) PRIMARY KEY(id)";
            connection.createTable(table, tableParams);
            connection.createChangeStream(stream, mode, table);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void createTableWithCustomParamsAndStream(Connection connection, PartitionMode mode, String table, String tableParams, String stream) {
        try {
            connection.createTable(table, tableParams);
            connection.createChangeStream(stream, mode, table);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "30"));
    }

    protected String getTopicName(Configuration config, String tableName) {
        String debeziumConnectorName = "testing-connector";
        return debeziumConnectorName + "." + tableName;
    }

    /**
     * The local Spanner emulator's PostgreSQL dialect support has an observed limitation where a
     * dropped change stream doesn't immediately free its slot against the emulator's per-database
     * cap of 10 concurrent change streams, so running this class's full dialect parameterization
     * (which creates and drops a PostgreSQL change stream in nearly every test) against a single
     * PostgreSQL-dialect database can hit that cap partway through the suite.
     *
     * <p>To keep full {@link Dialect} parameterization working reliably against the emulator, a
     * fresh PostgreSQL-dialect database is transparently rotated in (via
     * {@link #registerPgChangeStreamCreation()}) after every
     * {@value #MAX_PG_CHANGE_STREAMS_PER_DATABASE} change streams created against the current one.
     * {@code pgChangeStreamsCreatedOnCurrentDatabase} is an {@link AtomicInteger} and rotation is
     * performed under a lock, so this is safe even if tests in this class were ever run
     * concurrently (JUnit 5 parallel execution), not just sequentially as they run today.
     */
    private static final int MAX_PG_CHANGE_STREAMS_PER_DATABASE = 9;
    private static final AtomicInteger pgChangeStreamsCreatedOnCurrentDatabase = new AtomicInteger(0);
    private static final AtomicReference<Database> currentPgDatabase = new AtomicReference<>(pgDatabase);
    private static final AtomicReference<Connection> currentPgDatabaseConnection = new AtomicReference<>(pgDatabaseConnection);
    private static final AtomicReference<Configuration> currentBasePgConfig = new AtomicReference<>(basePgConfig);

    /**
     * Called once per test that's about to create a PostgreSQL-dialect change stream. Rotates in a
     * fresh PostgreSQL-dialect database (updating {@link #currentPgDatabase},
     * {@link #currentPgDatabaseConnection}, {@link #currentBasePgConfig}) once the current one has
     * had {@value #MAX_PG_CHANGE_STREAMS_PER_DATABASE} change streams created against it.
     */
    static synchronized void registerPgChangeStreamCreation(Logger logger) {
        if (pgChangeStreamsCreatedOnCurrentDatabase.incrementAndGet() > MAX_PG_CHANGE_STREAMS_PER_DATABASE) {
            Database freshDatabase = Database.builder()
                    .generateDatabaseId()
                    .dialect(Dialect.POSTGRESQL)
                    .build();
            Connection freshConnection = Connection.isRealSpanner()
                    ? RealSpannerTestSupport.getConnection(freshDatabase)
                    : freshDatabase.getConnection();
            Configuration freshConfig = Connection.isRealSpanner()
                    ? createBaseConfigBuilder(freshDatabase, true).build()
                    : Configuration.copy(currentBasePgConfig.get())
                            .with("gcp.spanner.instance.id", freshDatabase.getInstanceId())
                            .with("gcp.spanner.project.id", freshDatabase.getProjectId())
                            .with("gcp.spanner.database.id", freshDatabase.getDatabaseId())
                            .build();
            logger.info("Rotating to a fresh PostgreSQL-dialect database {} (from {}) after reaching the "
                    + "local emulator's per-database change stream cap",
                    freshDatabase.getDatabaseId(), currentPgDatabase.get().getDatabaseId());
            currentPgDatabase.set(freshDatabase);
            currentPgDatabaseConnection.set(freshConnection);
            currentBasePgConfig.set(freshConfig);
            pgChangeStreamsCreatedOnCurrentDatabase.set(1);
        }
    }

    /**
     * Resolves the {@link Connection} to use for a given {@link Dialect}, for tests parameterized
     * over dialect. For {@link Dialect#POSTGRESQL}, this is the trigger point for
     * {@link #registerPgChangeStreamCreation()}: callers are expected to call this once per test,
     * immediately before creating that test's change stream, and to reuse the returned
     * {@link Connection} for the rest of the test (including config building via
     * {@link #baseConfigFor}) so the connection and config always refer to the same database.
     */
    Connection connectionFor(Dialect dialect, Logger logger) {
        if (dialect == Dialect.POSTGRESQL) {
            registerPgChangeStreamCreation(logger);
            return currentPgDatabaseConnection.get();
        }
        return databaseConnection;
    }

    /**
     * Resolves the base {@link Configuration} to use for a given {@link Dialect}, for tests
     * parameterized over dialect. Must be called after {@link #connectionFor} in the same test, so
     * that if {@link #connectionFor} rotated in a fresh PostgreSQL-dialect database, this returns
     * the config matching that same database rather than a stale one.
     */
    Configuration baseConfigFor(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL ? currentBasePgConfig.get() : baseConfig;
    }

    /**
     * Suffixes a table name constant with the partition mode & dialect, so parameterized runs against
     * different modes/dialects don't collide on the same table name (mirrors the table-name suffixing
     * already used by {@code CrossPartitionSplitOrderingIT} for its {@code PartitionMode} parameterization).
     */
    static String tableFor(String tableNamePrefix, PartitionMode partitionMode, Dialect dialect) {
        String tableName = tableNamePrefix;

        if (partitionMode != null) {
            tableName += switch (partitionMode) {
                case IMMUTABLE_KEY_RANGE -> "_ikr";
                case MUTABLE_KEY_RANGE -> "_mkr";
            };
        }
        if (dialect != null) {
            tableName += switch (dialect) {
                case GOOGLE_STANDARD_SQL -> "_gsql";
                case POSTGRESQL -> "_pgsql";
            };
        }

        return tableName;
    }

    /**
     * Suffixes a change stream name constant with the partition mode & dialect, so parameterized runs against
     * different modes/dialects don't collide on the same change stream name.
     */
    static String streamFor(String streamNamePrefix, PartitionMode partitionMode, Dialect dialect) {
        String streamName = streamNamePrefix;
        if (partitionMode != null) {
            streamName += switch (partitionMode) {
                case IMMUTABLE_KEY_RANGE -> "Ikr";
                case MUTABLE_KEY_RANGE -> "Mkr";
            };
        }
        if (dialect != null) {
            streamName += switch (dialect) {
                case GOOGLE_STANDARD_SQL -> "Gsql";
                case POSTGRESQL -> "Pgsql";
            };
        }
        return streamName;
    }

    /**
     * The Spanner type name for a 64-bit integer column, which differs between dialects (used e.g.
     * for the mid-stream {@code ALTER TABLE ... ADD COLUMN} schema-change tests).
     */
    static String int64TypeFor(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL ? "bigint" : "INT64";
    }

    static Stream<Arguments> partitionModesAndDialects() {
        return Arrays.stream(PartitionMode.values())
                .flatMap(mode -> Arrays.stream(Dialect.values())
                        .map(dialect -> Arguments.of(mode, dialect)));
    }
}
