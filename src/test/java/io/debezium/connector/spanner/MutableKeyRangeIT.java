/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.util.Testing;

/**
 * Integration tests for mutable key range change streams.
 *
 * <p>This test is {@link RealSpannerCompatible}: when {@code -Dspanner.test.real=true} is passed it
 * runs against a real Cloud Spanner instance; otherwise it runs against the local emulator, which
 * supports {@code MUTABLE_KEY_RANGE} change streams directly. The forced-key-range-split tests
 * ({@link #shouldPreserveOrderAcrossForcedKeyRangeSplit} and
 * {@link #shouldNotLoseOrReorderEventsWhenStoppedDuringForcedKeyRangeSplit}) are the exception -
 * they self-skip on the emulator, which doesn't implement the {@code AddSplitPoints} admin RPC
 * that {@link Connection#forceSplit} relies on.
 *
 * <p>Run the whole suite against real Spanner with:
 * <pre>
 *   mvn verify \
 *     -Dspanner.test.real=true \
 *     -Dgcp.spanner.project.id=YOUR_PROJECT \
 *     -Dgcp.spanner.instance.id=YOUR_INSTANCE \
 *     -Dgcp.spanner.credentials.path=/path/to/key.json
 * </pre>
 *
 * <p>WINDOW_MINUTES is set to 1 so the sliding-window processedTimestamp
 * test completes in ~2 minutes instead of the production 20-minute default.
 */
@RealSpannerCompatible
public class MutableKeyRangeIT extends AbstractSpannerConnectorIT {

    /**
     * Override the inherited emulator connection/config with a real-Spanner pair when
     * {@code -Dspanner.test.real=true} is supplied; otherwise keep the parent's emulator pair.
     */
    protected static final Connection databaseConnection = Connection.isRealSpanner()
            ? RealSpannerTestSupport.getConnection(database)
            : AbstractSpannerConnectorIT.databaseConnection;
    protected static final Configuration baseConfig = Connection.isRealSpanner()
            ? createBaseConfigBuilder(database, true).build()
            : AbstractSpannerConnectorIT.baseConfig;

    /**
     * Same real-Spanner-or-emulator override as {@link #databaseConnection}/{@link #baseConfig}
     * above, but for the PostgreSQL-dialect pair, so tests parameterized over {@link Dialect} get a
     * real PostgreSQL-dialect connection too when {@code -Dspanner.test.real=true} is supplied.
     */
    protected static final Connection pgDatabaseConnection = Connection.isRealSpanner()
            ? RealSpannerTestSupport.getConnection(pgDatabase)
            : AbstractSpannerConnectorIT.pgDatabaseConnection;
    protected static final Configuration basePgConfig = Connection.isRealSpanner()
            ? createBaseConfigBuilder(pgDatabase, true).build()
            : AbstractSpannerConnectorIT.basePgConfig;

    private static final Logger LOG = LoggerFactory.getLogger(MutableKeyRangeIT.class);

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
    private static synchronized void registerPgChangeStreamCreation() {
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
            LOG.info("Rotating to a fresh PostgreSQL-dialect database {} (from {}) after reaching the "
                    + "local emulator's per-database change stream cap",
                    freshDatabase.getDatabaseId(), currentPgDatabase.get().getDatabaseId());
            currentPgDatabase.set(freshDatabase);
            currentPgDatabaseConnection.set(freshConnection);
            currentBasePgConfig.set(freshConfig);
            pgChangeStreamsCreatedOnCurrentDatabase.set(1);
        }
    }

    static {
        // Real Cloud Spanner change-stream reads plus this connector's task-sync/leader-election
        // bootstrap add latency the local emulator doesn't have, so the debezium-embedded defaults
        // (30s wait for the first record, then up to 3 x 10s of additional polling) are sometimes
        // too tight here. Raise the defaults for real-Spanner runs so the suite is stable without
        // requiring extra -D flags on the command line; explicit -D overrides still win.
        if (Connection.isRealSpanner()) {
            System.setProperty("debezium.test.records.waittime",
                    System.getProperty("debezium.test.records.waittime", "60"));
            System.setProperty("debezium.test.records.waittime.after.nulls",
                    System.getProperty("debezium.test.records.waittime.after.nulls", "5"));
        }
    }

    private static final String TABLE_CRUD = "mkr_crud_table";
    private static final String TABLE_RESTART = "mkr_restart_table";
    private static final String TABLE_WINDOW = "mkr_window_table";
    private static final String TABLE_ORDER = "mkr_order_table";
    private static final String TABLE_MID_WINDOW_STOP = "mkr_mid_window_stop_table";
    private static final String TABLE_MID_WINDOW_DELETE = "mkr_mid_window_delete_table";
    private static final String TABLE_QUIET_WINDOW = "mkr_quiet_window_table";
    private static final String TABLE_HISTORICAL_START = "mkr_historical_start_table";
    private static final String TABLE_SCHEMA_CHANGE = "mkr_schema_change_table";
    private static final String TABLE_SCHEMA_CHANGE_MID_STREAM = "mkr_schema_change_mid_stream_table";
    private static final String TABLE_LARGE_TRANSACTION = "mkr_large_transaction_table";
    private static final String TABLE_WINDOW_RECONFIG = "mkr_window_reconfig_table";
    private static final String TABLE_MOVE_IN_RESTART = "mkr_move_in_restart_table";

    private static final String STREAM_CRUD = "mkrCrudStream";
    private static final String STREAM_RESTART = "mkrRestartStream";
    private static final String STREAM_WINDOW = "mkrWindowStream";
    private static final String STREAM_ORDER = "mkrOrderStream";
    private static final String STREAM_MID_WINDOW_STOP = "mkrMidWindowStopStream";
    private static final String STREAM_MID_WINDOW_DELETE = "mkrMidWindowDeleteStream";
    private static final String STREAM_QUIET_WINDOW = "mkrQuietWindowStream";
    private static final String STREAM_HISTORICAL_START = "mkrHistoricalStartStream";
    private static final String STREAM_SCHEMA_CHANGE = "mkrSchemaChangeStream";
    private static final String STREAM_SCHEMA_CHANGE_MID_STREAM = "mkrSchemaChangeMidStreamStream";
    private static final String STREAM_LARGE_TRANSACTION = "mkrLargeTransactionStream";
    private static final String STREAM_WINDOW_RECONFIG = "mkrWindowReconfigStream";
    private static final String STREAM_MOVE_IN_RESTART = "mkrMoveInRestartStream";

    /**
     * Sliding-window size used during this IT run.
     * Keep at 1 for fast test runs; bump to 20 to mimic production behaviour.
     */
    private static final int WINDOW_MINUTES = 1;

    /**
     * Heartbeats are what advance the committed offset once a window/period has no new data.
     * The base config's 300s heartbeat is too slow for the short sliding window used here, so
     * shorten it to make sure the offset can advance past a window boundary within the test.
     */
    private static final String HEARTBEAT_INTERVAL_MS = "5000";
    private static final String OFFSET_FLUSH_INTERVAL_MS = "1000";

    /**
     * A much wider window used only by {@link #shouldNotLoseEventsWhenStoppedMidWindow}, so the
     * connector is stopped well before the window can naturally reach its real-time boundary.
     */
    private static final int WINDOW_MINUTES_FOR_MID_WINDOW_STOP = 5;

    /**
     * Resolves the {@link Connection} to use for a given {@link Dialect}, for tests parameterized
     * over dialect. For {@link Dialect#POSTGRESQL}, this is the trigger point for
     * {@link #registerPgChangeStreamCreation()}: callers are expected to call this once per test,
     * immediately before creating that test's change stream, and to reuse the returned
     * {@link Connection} for the rest of the test (including config building via
     * {@link #baseConfigFor}) so the connection and config always refer to the same database.
     */
    private Connection connectionFor(Dialect dialect) {
        if (dialect == Dialect.POSTGRESQL) {
            registerPgChangeStreamCreation();
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
    private Configuration baseConfigFor(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL ? currentBasePgConfig.get() : baseConfig;
    }

    /**
     * Suffixes a table name constant with the dialect, so parameterized runs against different
     * dialects don't collide on the same table name (mirrors the table-name suffixing already used
     * by {@code CrossPartitionSplitOrderingIT} for its {@code PartitionMode} parameterization).
     */
    private static String tableFor(String tableNamePrefix, Dialect dialect) {
        return tableNamePrefix + "_" + dialect.name().toLowerCase();
    }

    /**
     * Suffixes a change stream name constant with the dialect, so parameterized runs against
     * different dialects don't collide on the same change stream name.
     */
    private static String streamFor(String streamNamePrefix, Dialect dialect) {
        return streamNamePrefix + dialect.name();
    }

    /**
     * The Spanner type name for a 64-bit integer column, which differs between dialects (used e.g.
     * for the mid-stream {@code ALTER TABLE ... ADD COLUMN} schema-change tests).
     */
    private static String int64TypeFor(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL ? "bigint" : "INT64";
    }

    /**
     * Creates {@code table} (a plain {@code (id, name) PRIMARY KEY(id)} table, with dialect-
     * appropriate column types) and a MUTABLE_KEY_RANGE change stream over it, for a single test's
     * own setup. Used by tests parameterized over {@link Dialect} so the same scenario can run
     * against both a GoogleSQL- and a PostgreSQL-dialect database.
     */
    private void createMutableKeyRangeTableAndStream(Connection connection, Dialect dialect, String table, String stream) {
        try {
            String tableDefinition = dialect == Dialect.POSTGRESQL
                    ? table + "(id bigint, name varchar(100), PRIMARY KEY (id))"
                    : table + "(id INT64, name STRING(100)) PRIMARY KEY(id)";
            connection.createTable(tableDefinition);
            connection.createMutableKeyRangeChangeStream(stream, table);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * A different window size than {@link #WINDOW_MINUTES}, used only by
     * {@link #shouldResumeCorrectlyAfterWindowSizeIsChangedAcrossRestart} to verify the
     * connector doesn't assume a fixed window size across a restart.
     */
    private static final int RECONFIGURED_WINDOW_MINUTES = 3;

    @BeforeEach
    void initFramework() {
        clearKafkaTopics();
        deleteOffsetFiles();
        initializeConnectorTestFramework();
    }

    private static final String[] ALL_TABLE_NAME_PREFIXES = {
            TABLE_CRUD, TABLE_RESTART, TABLE_WINDOW, TABLE_ORDER, TABLE_MID_WINDOW_STOP,
            TABLE_MID_WINDOW_DELETE, TABLE_QUIET_WINDOW, TABLE_HISTORICAL_START, TABLE_SCHEMA_CHANGE,
            TABLE_SCHEMA_CHANGE_MID_STREAM, TABLE_LARGE_TRANSACTION, TABLE_WINDOW_RECONFIG, TABLE_MOVE_IN_RESTART
    };

    private static void deleteOffsetFiles() {
        for (String tableNamePrefix : ALL_TABLE_NAME_PREFIXES) {
            for (Dialect dialect : Dialect.values()) {
                String name = tableFor(tableNamePrefix, dialect) + "_connector";
                new File(System.getProperty("java.io.tmpdir"), "mkr-offsets-" + name + ".dat").delete();
            }
        }
    }

    private static String offsetFile(String connectorName) {
        return new File(System.getProperty("java.io.tmpdir"), "mkr-offsets-" + connectorName + ".dat").getAbsolutePath();
    }

    @AfterEach
    void ensureConnectorStopped() throws InterruptedException {
        stopConnector();
        assertConnectorNotRunning();
    }

    private Configuration buildConfig(String connectorName, String stream) {
        return buildConfig(connectorName, stream, WINDOW_MINUTES);
    }

    private Configuration buildConfig(String connectorName, String stream, int windowMinutes) {
        return buildConfig(baseConfig, connectorName, stream, windowMinutes);
    }

    /**
     * Dialect-aware variant that builds the config from an arbitrary base {@code Configuration}
     * (e.g. {@link #basePgConfig} for PostgreSQL-dialect tests), instead of always the GoogleSQL
     * {@link #baseConfig}.
     */
    private Configuration buildConfig(Configuration base, String connectorName, String stream, int windowMinutes) {
        return Configuration.copy(base)
                .with("gcp.spanner.change.stream", stream)
                .with("name", connectorName)
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .with("gcp.spanner.mutable.window.minutes", windowMinutes)
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", offsetFile(connectorName))
                .with("heartbeat.interval.ms", HEARTBEAT_INTERVAL_MS)
                .with("offset.flush.interval.ms", OFFSET_FLUSH_INTERVAL_MS)
                .build();
    }

    private String op(List<SourceRecord> records, int index) {
        return (String) ((Struct) records.get(index).value()).get("op");
    }

    /**
     * Polls for at least {@code minExpectedCount} records on {@code table}'s topic, accumulating
     * across repeated short polls until the deadline elapses. A single {@code waitForAvailableRecords}
     * + {@code consumeRecordsByTopic} call can race ahead of a real Cloud Spanner partition's
     * discovery/streaming latency (which varies run to run), so retrying within the overall budget
     * is more robust than a single one-shot wait.
     */
    private List<SourceRecord> consumeRecordsForTopic(Configuration config, String table, int minExpectedCount) throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(waitTimeForRecords());
        do {
            waitForAvailableRecords(5, TimeUnit.SECONDS);
            List<SourceRecord> polled = consumeRecordsByTopic(minExpectedCount - records.size(), false)
                    .recordsForTopic(getTopicName(config, table));
            if (polled != null) {
                records.addAll(polled);
            }
        } while (records.size() < minExpectedCount && System.nanoTime() < deadline);
        return records;
    }

    /**
     * Verifies that INSERT / UPDATE / DELETE produce c / u / d / tombstone records in order, for a
     * MUTABLE_KEY_RANGE change stream on both a GoogleSQL- and a PostgreSQL-dialect database.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldStreamCrudEventsToKafka(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(TABLE_CRUD, dialect);
        String stream = streamFor(STREAM_CRUD, dialect);

        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Configuration config = buildConfig(base, table + "_connector", stream, WINDOW_MINUTES);
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate("INSERT INTO " + table + " (id, name) VALUES (1, 'alpha')");
            connection.executeUpdate("UPDATE " + table + " SET name = 'beta' WHERE id = 1");
            connection.executeUpdate("DELETE FROM " + table + " WHERE id = 1");

            List<SourceRecord> records = consumeRecordsForTopic(config, table, 4);

            assertThat(records).as("Expected 4 records: c / u / d / tombstone").hasSize(4);
            assertThat(op(records, 0)).as("First record should be INSERT").isEqualTo("c");
            assertThat(op(records, 1)).as("Second record should be UPDATE").isEqualTo("u");
            assertThat(op(records, 2)).as("Third record should be DELETE").isEqualTo("d");
            assertThat(records.get(3).value()).as("Fourth record should be tombstone").isNull();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Verifies that after a graceful connector restart, id=11 is streamed.
     * At-least-once semantics: id=10 may be replayed once after restart.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldNotRepublishEventsAfterConnectorRestart(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect);
        String table = tableFor(TABLE_RESTART, dialect);
        String stream = streamFor(STREAM_RESTART, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream, WINDOW_MINUTES);
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate("INSERT INTO " + table + " (id, name) VALUES (10, 'pre-restart')");
            List<SourceRecord> before = consumeRecordsForTopic(config, table, 1);
            assertThat(before).as("Should have exactly 1 record before restart").hasSize(1);
            assertThat(op(before, 0)).isEqualTo("c");

            stopConnector();
            assertConnectorNotRunning();

            connection.executeUpdate("INSERT INTO " + table + " (id, name) VALUES (11, 'post-restart')");

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();
            // At-least-once semantics mean the replayed id=10 record can legitimately arrive before
            // id=11, so ask for up to 2 records: if id=10 is replayed we need both to see id=11; if it
            // isn't, we'll only get 1 and simply wait out the remaining budget before returning it.
            List<SourceRecord> after = consumeRecordsForTopic(config, table, 2);

            assertThat(after).as("Should have at least 1 record after restart").hasSizeGreaterThanOrEqualTo(1);
            for (SourceRecord r : after) {
                assertThat(((Struct) r.value()).getString("op")).isEqualTo("c");
                assertThat(((Struct) r.value()).getStruct("after").getInt64("id")).isIn(10L, 11L);
            }
            assertThat(after.stream()
                    .map(r -> ((Struct) r.value()).getStruct("after").getInt64("id"))
                    .collect(Collectors.toList()))
                    .as("id=11 must be present after restart").contains(11L);
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Verifies that after a full sliding window elapses and the connector is restarted
     * with no new data, the processedTimestamp prevents re-streaming already-seen events.
     *
     * <p>With WINDOW_MINUTES=1 this test waits roughly (WINDOW_MINUTES+1) minutes.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldNotReplayAfterWindowElapses(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect);
        String table = tableFor(TABLE_WINDOW, dialect);
        String stream = streamFor(STREAM_WINDOW, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream, WINDOW_MINUTES);
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate("INSERT INTO " + table + " (id, name) VALUES (20, 'window-seed')");
            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            consumeRecordsByTopic(5, false);

            Testing.print("Waiting " + (WINDOW_MINUTES + 1) + " minute(s) for sliding window to complete...");
            Thread.sleep(TimeUnit.MINUTES.toMillis(WINDOW_MINUTES + 1));

            stopConnector();
            assertConnectorNotRunning();

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            List<SourceRecord> replayed = consumeRecordsByTopic(5, false)
                    .recordsForTopic(getTopicName(config, table));

            assertThat(replayed)
                    .as("processedTimestamp should prevent replay of events from already-processed windows")
                    .isNullOrEmpty();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Verifies that, even while Spanner physically splits the key range mid-stream -
     * forcing the destination partitions to pause/resume per the "Partition Move-in/Move-out
     * Ordering" design (see {@code MoveInStateUpdateOperation}, {@code MoveOutStateUpdateOperation},
     * and {@code FindPartitionForStreamingOperation}) - records for a given primary key are still
     * delivered in the exact order they were written: no gaps, no duplicates, no reordering across
     * the split boundary.
     *
     * <p>Uses the {@code AddSplitPoints} admin API (see
     * https://cloud.google.com/spanner/docs/create-manage-split-points), which requires the
     * {@code spanner.databases.addSplitPoints} permission (granted by the
     * {@code roles/spanner.databaseAdmin} IAM role).
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldPreserveOrderAcrossForcedKeyRangeSplit(Dialect dialect) throws InterruptedException, ExecutionException {
        Assumptions.assumeTrue(Connection.isRealSpanner(),
                "Skipping: the local Spanner emulator doesn't implement the AddSplitPoints admin RPC "
                        + "(UNIMPLEMENTED) that forceSplit relies on. Run with -Dspanner.test.real=true "
                        + "to exercise this test.");
        Connection connection = connectionFor(dialect);
        String table = tableFor(TABLE_ORDER, dialect);
        String stream = streamFor(STREAM_ORDER, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream, WINDOW_MINUTES);
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            long[] keys = { 500L, 1500L, 2500L };
            int updatesPerKey = 5;

            for (long key : keys) {
                connection.executeUpdate(
                        "INSERT INTO " + table + " (id, name) VALUES (" + key + ", 'v0')");
            }

            // Force the key range to split around each key, right as further updates are issued,
            // to exercise the destination partitions' MoveIn pause/resume logic mid-stream.
            // Short expiry: this test finishes in well under 2 minutes, and split points count
            // against a small, instance-wide quota on the shared real-Spanner test instance.
            connection.forceSplit(table, Duration.ofMinutes(10), "1000");
            connection.forceSplit(table, Duration.ofMinutes(10), "2000");

            for (int i = 1; i <= updatesPerKey; i++) {
                for (long key : keys) {
                    connection.executeUpdate(
                            "UPDATE " + table + " SET name = 'v" + i + "' WHERE id = " + key);
                }
            }

            // A single consumeRecordsByTopic call can return before all records have propagated
            // through a pause/resume cycle triggered by the forced split, so accumulate across
            // repeated polls until either the expected count arrives or the deadline is reached.
            int expectedCount = keys.length * (updatesPerKey + 1);
            List<SourceRecord> records = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(waitTimeForRecords());
            while (records.size() < expectedCount && System.nanoTime() < deadline) {
                waitForAvailableRecords(5, TimeUnit.SECONDS);
                List<SourceRecord> polled = consumeRecordsByTopic(expectedCount - records.size(), false)
                        .recordsForTopic(getTopicName(config, table));
                if (polled != null) {
                    records.addAll(polled);
                }
            }

            Map<Long, List<SourceRecord>> byKey = records.stream()
                    .collect(Collectors.groupingBy(r -> ((Struct) r.value()).getStruct("after").getInt64("id")));

            List<String> expected = IntStream.rangeClosed(0, updatesPerKey)
                    .mapToObj(i -> "v" + i)
                    .collect(Collectors.toList());

            for (long key : keys) {
                List<String> namesInOrder = byKey.getOrDefault(key, List.of()).stream()
                        .map(r -> ((Struct) r.value()).getStruct("after").getString("name"))
                        .collect(Collectors.toList());

                assertThat(namesInOrder)
                        .as("Records for id=%d must arrive in write order despite the forced key range split", key)
                        .isEqualTo(expected);
            }
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Verifies that stopping the connector as soon as possible after a forced key range split -
     * potentially mid-way through the MoveIn/MoveOut pause-and-resume handshake exercised by
     * {@link #shouldPreserveOrderAcrossForcedKeyRangeSplit} - does not lose or reorder events on
     * restart. The MoveIn state that gates a paused destination partition is persisted via
     * {@code PartitionState}/the sync topic (see {@code MoveInStateUpdateOperation}), so it must
     * survive a stop/start cycle the same way {@code processedTimestamp} and
     * {@code lastBoundaryRecordSequence} already do elsewhere.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldNotLoseOrReorderEventsWhenStoppedDuringForcedKeyRangeSplit(Dialect dialect) throws InterruptedException, ExecutionException {
        Assumptions.assumeTrue(Connection.isRealSpanner(),
                "Skipping: the local Spanner emulator doesn't implement the AddSplitPoints admin RPC "
                        + "(UNIMPLEMENTED) that forceSplit relies on. Run with -Dspanner.test.real=true "
                        + "to exercise this test.");
        Connection connection = connectionFor(dialect);
        String table = tableFor(TABLE_MOVE_IN_RESTART, dialect);
        String stream = streamFor(STREAM_MOVE_IN_RESTART, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream, WINDOW_MINUTES);
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            long key = 1500L;
            connection.executeUpdate(
                    "INSERT INTO " + table + " (id, name) VALUES (" + key + ", 'v0')");

            // Force a split, then stop immediately - no settle time - to maximize the chance the
            // connector is caught somewhere in the middle of the MoveIn/MoveOut handshake rather
            // than safely resolved beforehand. Short expiry: this test finishes in well under
            // 2 minutes, and split points count against a small, instance-wide quota on the
            // shared real-Spanner test instance.
            connection.forceSplit(table, Duration.ofMinutes(10), "1000");
            stopConnector();
            assertConnectorNotRunning();

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            int updatesAfterRestart = 5;
            for (int i = 1; i <= updatesAfterRestart; i++) {
                connection.executeUpdate(
                        "UPDATE " + table + " SET name = 'v" + i + "' WHERE id = " + key);
            }

            // Waiting for a raw record count isn't reliable here: at-least-once redelivery of
            // earlier values (v0..v4) can inflate the count to "expected" before the genuinely
            // final "v5" write has actually arrived, causing the loop to stop polling too early.
            // Poll until the last expected value has actually been seen, or the deadline passes.
            String finalExpectedValue = "v" + updatesAfterRestart;
            List<SourceRecord> records = new ArrayList<>();
            long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(waitTimeForRecords() * 3);
            boolean sawFinalValue = false;
            while (!sawFinalValue && System.currentTimeMillis() < deadline) {
                waitForAvailableRecords(5, TimeUnit.SECONDS);
                List<SourceRecord> batch = consumeRecordsByTopic(20, false)
                        .recordsForTopic(getTopicName(config, table));
                if (batch != null) {
                    records.addAll(batch);
                    sawFinalValue = batch.stream()
                            .filter(r -> r.value() != null)
                            .anyMatch(r -> finalExpectedValue.equals(((Struct) r.value()).getStruct("after").getString("name")));
                }
            }

            List<String> namesInOrder = records.stream()
                    .filter(r -> r.value() != null)
                    .map(r -> ((Struct) r.value()).getStruct("after").getString("name"))
                    .collect(Collectors.toList());

            List<String> expected = IntStream.rangeClosed(0, updatesAfterRestart)
                    .mapToObj(i -> "v" + i)
                    .collect(Collectors.toList());

            // containsSubsequence (not isEqualTo): at-least-once semantics permit a value to be
            // redelivered after the restart, but every expected value must still appear, in order.
            assertThat(namesInOrder)
                    .as("Every write for id=%d must appear, in order, even though the connector was "
                            + "stopped as soon as possible after a forced key range split - whatever "
                            + "state the MoveIn/MoveOut handshake was caught in must resume correctly, "
                            + "not lose or reorder data", key)
                    .containsSubsequence(expected);
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Verifies that stopping the connector while a sliding window is still open - with events
     * inserted but the window nowhere near its real-time boundary yet - does not lose those
     * events on restart. A much wider window than the other tests use is deliberately chosen so
     * the stop reliably lands mid-window rather than racing a window boundary that might close
     * naturally first.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldNotLoseEventsWhenStoppedMidWindow(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect);
        String table = tableFor(TABLE_MID_WINDOW_STOP, dialect);
        String stream = streamFor(STREAM_MID_WINDOW_STOP, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream,
                    WINDOW_MINUTES_FOR_MID_WINDOW_STOP);
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            for (long id = 1; id <= 5; id++) {
                connection.executeUpdate(
                        "INSERT INTO " + table + " (id, name) VALUES (" + id + ", 'row-" + id + "')");
            }

            // Give the connector time to open its window query and start delivering some of the
            // 5 rows, without waiting anywhere near WINDOW_MINUTES_FOR_MID_WINDOW_STOP minutes for
            // the window to close naturally. Deliberately not draining the topic here: whatever did
            // or didn't make it through before the stop stays there to be checked after restart.
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));

            stopConnector();
            assertConnectorNotRunning();

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            List<SourceRecord> records = consumeRecordsForTopic(config, table, 5);

            List<Long> idsDelivered = records.stream()
                    .filter(r -> r.value() != null && "c".equals(((Struct) r.value()).get("op")))
                    .map(r -> ((Struct) r.value()).getStruct("after").getInt64("id"))
                    .collect(Collectors.toList());

            assertThat(idsDelivered)
                    .as("All 5 rows inserted before the mid-window stop must eventually be delivered - "
                            + "whatever wasn't consumed before the stop must not be skipped after restart")
                    .contains(1L, 2L, 3L, 4L, 5L);
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Verifies that a DELETE (and its tombstone) issued while a sliding window is still open is
     * not lost if the connector is stopped and restarted before that window closes naturally.
     * Mirrors {@link #shouldNotLoseEventsWhenStoppedMidWindow}, but for the DELETE/tombstone
     * path specifically rather than INSERT, since a delete's mod carries only old_values and is
     * mapped differently than a create.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldNotLoseDeleteWhenStoppedMidWindow(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect);
        String table = tableFor(TABLE_MID_WINDOW_DELETE, dialect);
        String stream = streamFor(STREAM_MID_WINDOW_DELETE, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream,
                    WINDOW_MINUTES_FOR_MID_WINDOW_STOP);
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + " (id, name) VALUES (1, 'row-1')");
            connection.executeUpdate(
                    "DELETE FROM " + table + " WHERE id = 1");

            // Same rationale as shouldNotLoseEventsWhenStoppedMidWindow: give the connector a moment
            // to start delivering, without waiting anywhere near WINDOW_MINUTES_FOR_MID_WINDOW_STOP
            // minutes for the window to close naturally. Deliberately not draining the topic here.
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));

            stopConnector();
            assertConnectorNotRunning();

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            List<SourceRecord> records = consumeRecordsForTopic(config, table, 3);

            boolean sawInsert = records.stream()
                    .anyMatch(r -> r.value() != null && "c".equals(((Struct) r.value()).get("op")));
            boolean sawDelete = records.stream()
                    .anyMatch(r -> r.value() != null && "d".equals(((Struct) r.value()).get("op")));
            boolean sawTombstone = records.stream().anyMatch(r -> r.value() == null);

            assertThat(sawInsert)
                    .as("The INSERT that preceded the mid-window stop must not be skipped after restart")
                    .isTrue();
            assertThat(sawDelete)
                    .as("The DELETE that preceded the mid-window stop must not be skipped after restart")
                    .isTrue();
            assertThat(sawTombstone)
                    .as("The tombstone following the DELETE must not be skipped after restart")
                    .isTrue();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Verifies that a single Spanner transaction containing many mods (as opposed to the
     * single-row transactions every other test in this class uses) is delivered completely, in
     * commit order, all sharing the same transaction id.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldDeliverAllModsFromLargeSingleTransaction(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect);
        String table = tableFor(TABLE_LARGE_TRANSACTION, dialect);
        String stream = streamFor(STREAM_LARGE_TRANSACTION, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream, WINDOW_MINUTES);
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            int rowCount = 20;
            List<String> inserts = new ArrayList<>();
            for (long id = 1; id <= rowCount; id++) {
                inserts.add("INSERT INTO " + table + " (id, name) VALUES (" + id + ", 'row-" + id + "')");
            }
            connection.executeUpdate(inserts);

            // A 20-mod burst can take longer to fully drain than consumeRecordsByTopic's short
            // default patience for a single poll. Poll repeatedly and accumulate across calls, since
            // each call only drains records newly arrived since the previous drain.
            List<SourceRecord> records = new ArrayList<>();
            long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(waitTimeForRecords() * 3);
            while (records.size() < rowCount && System.currentTimeMillis() < deadline) {
                waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
                List<SourceRecord> batch = consumeRecordsByTopic(rowCount + 5, false)
                        .recordsForTopic(getTopicName(config, table));
                if (batch != null) {
                    records.addAll(batch);
                }
            }

            assertThat(records)
                    .as("All %d mods from the single transaction must be delivered", rowCount)
                    .hasSize(rowCount);

            List<Long> idsDelivered = records.stream()
                    .map(r -> ((Struct) r.value()).getStruct("after").getInt64("id"))
                    .collect(Collectors.toList());
            List<Long> expectedIds = new ArrayList<>();
            for (long id = 1; id <= rowCount; id++) {
                expectedIds.add(id);
            }
            assertThat(idsDelivered)
                    .as("Every row from the transaction must be present, in commit order")
                    .containsExactlyElementsOf(expectedIds);

            List<String> transactionIds = records.stream()
                    .map(r -> ((Struct) r.value()).getStruct("source").getString("server_transaction_id"))
                    .distinct()
                    .collect(Collectors.toList());
            assertThat(transactionIds)
                    .as("All mods from one commit must be tagged with the same transaction id")
                    .hasSize(1);
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Verifies that a sliding window with no data changes in it - only heartbeats - still
     * closes and advances normally rather than stalling the connector. If the window boundary
     * logic got stuck re-querying the same empty window instead of moving on, a row inserted
     * afterward would never be delivered, since the connector would never reach a window that
     * covers it.
     *
     * <p>With WINDOW_MINUTES=1 this test waits roughly (WINDOW_MINUTES+1) minutes before
     * inserting anything.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldAdvanceThroughQuietWindowWithoutStalling(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect);
        String table = tableFor(TABLE_QUIET_WINDOW, dialect);
        String stream = streamFor(STREAM_QUIET_WINDOW, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream, WINDOW_MINUTES);
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            Testing.print("Waiting " + (WINDOW_MINUTES + 1) + " minute(s) for a quiet window to elapse...");
            Thread.sleep(TimeUnit.MINUTES.toMillis(WINDOW_MINUTES + 1));

            // Heartbeats emitted during the quiet window are real SourceRecords too, so they
            // accumulate in the framework's internal queue. Drain them first, otherwise
            // waitForAvailableRecords() below would pass immediately on that leftover heartbeat
            // activity instead of actually waiting for the row inserted next.
            consumeRecordsByTopic(50, false);

            connection.executeUpdate(
                    "INSERT INTO " + table + " (id, name) VALUES (30, 'after-quiet-window')");

            List<SourceRecord> records = consumeRecordsForTopic(config, table, 1);

            assertThat(records)
                    .as("Row inserted after a fully quiet window should still be delivered - "
                            + "the connector appears to have stalled advancing past the empty window")
                    .hasSize(1);
            assertThat(op(records, 0)).isEqualTo("c");
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Verifies that a connector refuses to start when gcp.spanner.mutable.window.minutes is set
     * below the validated range (must be between 1 and 30 inclusive), mirroring the same
     * validation already checked at the unit level by
     * BaseSpannerConnectorConfigTest.testMutableWindowMinutesValidation.
     *
     * <p>Doesn't create a real table/stream: this config value is validated before the
     * connector ever attempts to reach Spanner, so {@code TABLE_CRUD}/{@code STREAM_CRUD} here
     * are used only as configuration string values, not backed by real Spanner resources.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldNotStartConnectorWithWindowMinutesTooLow(Dialect dialect) throws InterruptedException {
        String table = tableFor(TABLE_CRUD, dialect);
        String stream = streamFor(STREAM_CRUD, dialect);
        Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream, 0);
        start(SpannerConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(msg).contains("Must be between 1 and 30 minutes");
        });
        assertConnectorNotRunning();
    }

    /**
     * Verifies that a connector refuses to start when gcp.spanner.mutable.window.minutes is set
     * above the validated range (must be between 1 and 30 inclusive).
     *
     * <p>Doesn't create a real table/stream, for the same reason as
     * {@link #shouldNotStartConnectorWithWindowMinutesTooLow}.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldNotStartConnectorWithWindowMinutesTooHigh(Dialect dialect) throws InterruptedException {
        String table = tableFor(TABLE_CRUD, dialect);
        String stream = streamFor(STREAM_CRUD, dialect);
        Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream, 31);
        start(SpannerConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(msg).contains("Must be between 1 and 30 minutes");
        });
        assertConnectorNotRunning();
    }

    /**
     * Verifies that starting the connector with gcp.spanner.start.time set several minutes in
     * the past forces it to catch up through multiple already-elapsed windows quickly, rather
     * than pacing one window per real-time minute the way a live-tailing connector naturally
     * would. Data is inserted first, then the connector isn't started until several minutes
     * later with a start time pointed back at those inserts, so several window boundaries have
     * already passed in real time before the connector ever begins reading.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldCatchUpQuicklyThroughHistoricalWindows(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect);
        String table = tableFor(TABLE_HISTORICAL_START, dialect);
        String stream = streamFor(STREAM_HISTORICAL_START, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Instant historicalStart = Instant.now();
            for (long id = 1; id <= 3; id++) {
                connection.executeUpdate(
                        "INSERT INTO " + table + " (id, name) VALUES (" + id + ", 'historical-" + id + "')");
            }

            int minutesInPast = WINDOW_MINUTES * 3;
            Testing.print("Waiting " + minutesInPast + " minute(s) so gcp.spanner.start.time is well in the past before starting...");
            Thread.sleep(TimeUnit.MINUTES.toMillis(minutesInPast));

            Configuration config = Configuration.copy(baseConfigFor(dialect))
                    .with("gcp.spanner.change.stream", stream)
                    .with("name", table + "_connector")
                    .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(historicalStart))
                    .with("gcp.spanner.mutable.window.minutes", WINDOW_MINUTES)
                    .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                    .with("offset.storage.file.filename", offsetFile(table + "_connector"))
                    .build();

            long startedAtMillis = System.currentTimeMillis();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // A single consumeRecordsByTopic call can return before all 3 historical rows have
            // been delivered - and recordsForTopic() returns null (a bare Map.get()) rather than
            // an empty list when nothing has arrived for this topic yet - so poll repeatedly and
            // accumulate, same as the other tests in this class that consume bursts of records.
            List<SourceRecord> records = new ArrayList<>();
            long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(waitTimeForRecords() * 3);
            while (records.size() < 3 && System.currentTimeMillis() < deadline) {
                waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
                List<SourceRecord> batch = consumeRecordsByTopic(10, false)
                        .recordsForTopic(getTopicName(config, table));
                if (batch != null) {
                    records.addAll(batch);
                }
            }
            long elapsedMillis = System.currentTimeMillis() - startedAtMillis;

            assertTrue(!records.isEmpty(),
                    "Historical rows inserted " + minutesInPast + " minute(s) before the connector started were never delivered");

            List<Long> idsDelivered = records.stream()
                    .filter(r -> r.value() != null && "c".equals(((Struct) r.value()).get("op")))
                    .map(r -> ((Struct) r.value()).getStruct("after").getInt64("id"))
                    .collect(Collectors.toList());

            assertThat(idsDelivered)
                    .as("All 3 historical rows must be delivered even though they predate the connector's own startup")
                    .contains(1L, 2L, 3L);
            assertThat(elapsedMillis)
                    .as("Catching up through several already-elapsed windows took %dms - a connector pacing "
                            + "one window per real-time minute instead of catching up immediately would take "
                            + "at least %d minute(s)", elapsedMillis, minutesInPast)
                    .isLessThan(TimeUnit.MINUTES.toMillis(minutesInPast));
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Verifies that a mid-stream schema change (ALTER TABLE ADD COLUMN) is picked up
     * automatically under MUTABLE_KEY_RANGE mode, without reconfiguring or restarting the
     * connector - including backfilling the new column on a row that predates the ALTER.
     *
     * <p>Confirmed passing against a real Cloud Spanner instance ({@code -Preal-spanner}).
     * See {@link #shouldPickUpSchemaChangeMidStreamForNewInserts} for why this test self-skips
     * when run against Spanner Omni instead.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldPickUpSchemaChangeMidStream(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect);
        String table = tableFor(TABLE_SCHEMA_CHANGE, dialect);
        String stream = streamFor(STREAM_SCHEMA_CHANGE, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream, WINDOW_MINUTES);
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + " (id, name) VALUES (1, 'Alice')");

            // Schema change happens mid-stream, without touching the change stream's own
            // configuration or restarting the connector.
            connection.updateDDL(List.of(
                    "ALTER TABLE " + table + " ADD COLUMN age " + int64TypeFor(dialect)));

            connection.executeUpdate(
                    "INSERT INTO " + table + " (id, name, age) VALUES (2, 'Bob', 30)");
            connection.executeUpdate(
                    "UPDATE " + table + " SET age = 99 WHERE id = 1");

            List<SourceRecord> records = new ArrayList<>();
            long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(waitTimeForRecords() * 3);
            while (records.size() < 3 && System.currentTimeMillis() < deadline) {
                waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
                List<SourceRecord> batch = consumeRecordsByTopic(10, false)
                        .recordsForTopic(getTopicName(config, table));
                if (batch != null) {
                    records.addAll(batch);
                }
            }
            assertThat(records).hasSize(3);

            Struct preAlterInsert = (Struct) records.get(0).value();
            assertThat(preAlterInsert.get("op")).isEqualTo("c");
            assertThat(preAlterInsert.getStruct("after").getString("name")).isEqualTo("Alice");

            Struct postAlterInsert = (Struct) records.get(1).value();
            assertThat(postAlterInsert.get("op")).isEqualTo("c");
            Struct postAlterAfter = postAlterInsert.getStruct("after");
            assertThat(postAlterAfter.getString("name")).isEqualTo("Bob");
            assertThat(postAlterAfter.getInt64("age")).isEqualTo(30L);

            // Existing row, updated after the column was added: the new column must be usable
            // without restarting or reconfiguring the connector.
            Struct backfillUpdate = (Struct) records.get(2).value();
            assertThat(backfillUpdate.get("op")).isEqualTo("u");
            assertThat(backfillUpdate.getStruct("after").getInt64("age")).isEqualTo(99L);
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Subset of {@link #shouldPickUpSchemaChangeMidStream}: verifies that a mid-stream schema
     * change (ALTER TABLE ADD COLUMN) is picked up automatically under MUTABLE_KEY_RANGE mode
     * for rows inserted after the change, without reconfiguring or restarting the connector.
     * Deliberately stops short of updating the pre-existing row afterward, since Spanner Omni
     * has a reproducible gap where that specific UPDATE never gets a change-stream record once
     * a table has a third column of type INT64 - confirmed Omni-specific (a STRING third column
     * with the same insert/insert/update pattern doesn't reproduce it), not a connector bug.
     * {@link #shouldPickUpSchemaChangeMidStream} self-skips for this reason when run against
     * Omni; this test exists so Omni runs still get some coverage of mid-stream schema changes.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldPickUpSchemaChangeMidStreamForNewInserts(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect);
        String table = tableFor(TABLE_SCHEMA_CHANGE_MID_STREAM, dialect);
        String stream = streamFor(STREAM_SCHEMA_CHANGE_MID_STREAM, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            Configuration config = buildConfig(baseConfigFor(dialect), table + "_connector", stream, WINDOW_MINUTES);
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + " (id, name) VALUES (1, 'Alice')");

            // Schema change happens mid-stream, without touching the change stream's own
            // configuration or restarting the connector.
            connection.updateDDL(List.of(
                    "ALTER TABLE " + table + " ADD COLUMN age " + int64TypeFor(dialect)));

            connection.executeUpdate(
                    "INSERT INTO " + table + " (id, name, age) VALUES (2, 'Bob', 30)");

            List<SourceRecord> records = consumeRecordsForTopic(config, table, 2);
            assertThat(records).hasSize(2);

            Struct preAlterInsert = (Struct) records.get(0).value();
            assertThat(preAlterInsert.get("op")).isEqualTo("c");
            assertThat(preAlterInsert.getStruct("after").getString("name")).isEqualTo("Alice");

            Struct postAlterInsert = (Struct) records.get(1).value();
            assertThat(postAlterInsert.get("op")).isEqualTo("c");
            Struct postAlterAfter = postAlterInsert.getStruct("after");
            assertThat(postAlterAfter.getString("name")).isEqualTo("Bob");
            assertThat(postAlterAfter.getInt64("age")).isEqualTo(30L);
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Verifies that resuming a partition with a different {@code gcp.spanner.mutable.window.minutes}
     * value than it was originally started with does not break streaming. The window size lives
     * only in the running {@code SpannerChangeStreamService} instance, not in the persisted
     * partition/offset state, so a restart with a changed value must still correctly compute the
     * next window from wherever the partition left off.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void shouldResumeCorrectlyAfterWindowSizeIsChangedAcrossRestart(Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(TABLE_WINDOW_RECONFIG, dialect);
        String stream = streamFor(STREAM_WINDOW_RECONFIG, dialect);
        createMutableKeyRangeTableAndStream(connection, dialect, table, stream);
        try {
            String connectorName = table + "_connector";
            Configuration initialConfig = buildConfig(base, connectorName, stream, WINDOW_MINUTES);
            start(SpannerConnector.class, initialConfig);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + " (id, name) VALUES (1, 'before-reconfig')");

            List<SourceRecord> before = consumeRecordsForTopic(initialConfig, table, 1);
            assertThat(before)
                    .as("Row inserted before the restart must be delivered under the original window size")
                    .hasSize(1);

            stopConnector();
            assertConnectorNotRunning();

            // Same connector name and offset file as before - a genuine resume of the same
            // partition - but a different window size than it was originally started with.
            Configuration reconfiguredConfig = buildConfig(base, connectorName, stream, RECONFIGURED_WINDOW_MINUTES);
            start(SpannerConnector.class, reconfiguredConfig);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + " (id, name) VALUES (2, 'after-reconfig')");

            // minExpectedCount=2, not 1: consumeRecordsForTopic stops polling once it hits the count,
            // so requesting just 1 risks returning on a redelivered id=1 before id=2 - the record
            // actually under test - ever arrives.
            List<SourceRecord> after = consumeRecordsForTopic(reconfiguredConfig, table, 2);

            // At-least-once semantics: id=1's insert may be redelivered alongside id=2's if it
            // hadn't been fully acknowledged before the stop, same as elsewhere in this class
            // (see shouldNotRepublishEventsAfterConnectorRestart). The claim under test is that
            // id=2 - inserted only after resuming with the new window size - is delivered at
            // all, not that id=1 is never seen again.
            assertThat(after)
                    .as("Row inserted after resuming with a different window size must still be delivered - "
                            + "the partition's persisted state must not assume a fixed window size across restarts")
                    .isNotEmpty();
            for (SourceRecord r : after) {
                assertThat(((Struct) r.value()).get("op")).isEqualTo("c");
                assertThat(((Struct) r.value()).getStruct("after").getInt64("id")).isIn(1L, 2L);
            }
            assertThat(after.stream()
                    .map(r -> ((Struct) r.value()).getStruct("after").getInt64("id"))
                    .collect(Collectors.toList()))
                    .as("id=2 must be present after resuming with the new window size").contains(2L);
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }
}
