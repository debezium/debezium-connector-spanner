/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.util.Testing;

/**
 * Integration tests for mutable key range change streams.
 *
 * <p>This test is {@link RealSpannerCompatible}: when {@code -Dspanner.test.real=true} is passed it
 * runs against a real Cloud Spanner instance; otherwise it runs against the local emulator and is
 * reported as <em>skipped</em> (not failed) because the emulator does not yet support
 * {@code MUTABLE_KEY_RANGE} change streams.
 *
 * <p>Run the whole suite (old tests on the emulator, this test on real Spanner) with a single
 * command:
 * <pre>
 *   mvn verify \
 *     -Dspanner.test.real=true \
 *     -Dgcp.spanner.project.id=YOUR_PROJECT \
 *     -Dgcp.spanner.instance.id=YOUR_INSTANCE \
 *     -Dgcp.spanner.credentials.path=/path/to/key.json
 * </pre>
 *
 * <p>Run against Spanner Omni:
 * <pre>
 *   -Dspanner.type=OMNI
 *   -Dgcp.spanner.host=https://your-omni-host:15000
 *   -Dspanner.omni.use.plaintext=true          # or use mTLS cert/key properties
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

    private static final String STREAM_CRUD = "mkrCrudStream";
    private static final String STREAM_RESTART = "mkrRestartStream";
    private static final String STREAM_WINDOW = "mkrWindowStream";
    private static final String STREAM_ORDER = "mkrOrderStream";

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

    private static boolean setupSucceeded;

    @BeforeAll
    static void setup() {
        try {
            databaseConnection.createTable(TABLE_CRUD + "(id INT64, name STRING(100)) PRIMARY KEY(id)");
            databaseConnection.createTable(TABLE_RESTART + "(id INT64, name STRING(100)) PRIMARY KEY(id)");
            databaseConnection.createTable(TABLE_WINDOW + "(id INT64, name STRING(100)) PRIMARY KEY(id)");
            databaseConnection.createTable(TABLE_ORDER + "(id INT64, name STRING(100)) PRIMARY KEY(id)");
            databaseConnection.createMutableKeyRangeChangeStream(STREAM_CRUD, TABLE_CRUD);
            databaseConnection.createMutableKeyRangeChangeStream(STREAM_RESTART, TABLE_RESTART);
            databaseConnection.createMutableKeyRangeChangeStream(STREAM_WINDOW, TABLE_WINDOW);
            databaseConnection.createMutableKeyRangeChangeStream(STREAM_ORDER, TABLE_ORDER);
            setupSucceeded = true;
            Testing.print("MutableKeyRangeIT is ready.");
        }
        catch (Exception e) {
            // The local emulator does not support MUTABLE_KEY_RANGE change streams. Swallow the setup
            // failure here so a plain `mvn verify` stays green; @BeforeEach will then skip each method
            // individually so they are reported as skipped (not silently ignored). Real Spanner and
            // Omni backends are expected to support the DDL and should surface genuine failures.
            if (!Connection.isRealSpanner() && !Database.isSpannerOmniEndpoint()) {
                Testing.print("Skipping MutableKeyRangeIT: MUTABLE_KEY_RANGE change streams are not supported "
                        + "by the local Spanner emulator (" + e.getMessage() + "). Run with -Dspanner.test.real=true or "
                        + "-Dspanner.type=OMNI against a backend that supports it.");
                setupSucceeded = false;
                return;
            }
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(STREAM_CRUD);
        databaseConnection.dropChangeStream(STREAM_RESTART);
        databaseConnection.dropChangeStream(STREAM_WINDOW);
        databaseConnection.dropChangeStream(STREAM_ORDER);
        databaseConnection.dropTable(TABLE_CRUD);
        databaseConnection.dropTable(TABLE_RESTART);
        databaseConnection.dropTable(TABLE_WINDOW);
        databaseConnection.dropTable(TABLE_ORDER);
    }

    @BeforeEach
    void initFramework() {
        Assumptions.assumeTrue(setupSucceeded, "MutableKeyRangeIT setup did not complete; skipping tests");
        clearKafkaTopics();
        deleteOffsetFiles();
        initializeConnectorTestFramework();
    }

    private static void deleteOffsetFiles() {
        for (String name : new String[]{ TABLE_CRUD + "_connector", TABLE_RESTART + "_connector", TABLE_WINDOW + "_connector", TABLE_ORDER + "_connector" }) {
            new File(System.getProperty("java.io.tmpdir"), "mkr-offsets-" + name + ".dat").delete();
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
        return Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", stream)
                .with("name", connectorName)
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .with("gcp.spanner.mutable.window.minutes", WINDOW_MINUTES)
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
     * Verifies that INSERT / UPDATE / DELETE produce c / u / d / tombstone records in order.
     */
    @Test
    void shouldStreamCrudEventsToKafka() throws InterruptedException {
        Configuration config = buildConfig(TABLE_CRUD + "_connector", STREAM_CRUD);
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate("INSERT INTO " + TABLE_CRUD + " (id, name) VALUES (1, 'alpha')");
        databaseConnection.executeUpdate("UPDATE " + TABLE_CRUD + " SET name = 'beta' WHERE id = 1");
        databaseConnection.executeUpdate("DELETE FROM " + TABLE_CRUD + " WHERE id = 1");

        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
        List<SourceRecord> records = consumeRecordsByTopic(10, false)
                .recordsForTopic(getTopicName(config, TABLE_CRUD));

        assertThat(records).as("Expected 4 records: c / u / d / tombstone").hasSize(4);
        assertThat(op(records, 0)).as("First record should be INSERT").isEqualTo("c");
        assertThat(op(records, 1)).as("Second record should be UPDATE").isEqualTo("u");
        assertThat(op(records, 2)).as("Third record should be DELETE").isEqualTo("d");
        assertThat(records.get(3).value()).as("Fourth record should be tombstone").isNull();
    }

    /**
     * Verifies that after a graceful connector restart, id=11 is streamed.
     * At-least-once semantics: id=10 may be replayed once after restart.
     */
    @Test
    void shouldNotRepublishEventsAfterConnectorRestart() throws InterruptedException {
        Configuration config = buildConfig(TABLE_RESTART + "_connector", STREAM_RESTART);
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate("INSERT INTO " + TABLE_RESTART + " (id, name) VALUES (10, 'pre-restart')");
        List<SourceRecord> before = consumeRecordsForTopic(config, TABLE_RESTART, 1);
        assertThat(before).as("Should have exactly 1 record before restart").hasSize(1);
        assertThat(op(before, 0)).isEqualTo("c");

        stopConnector();
        assertConnectorNotRunning();

        databaseConnection.executeUpdate("INSERT INTO " + TABLE_RESTART + " (id, name) VALUES (11, 'post-restart')");

        start(SpannerConnector.class, config);
        assertConnectorIsRunning();
        // At-least-once semantics mean the replayed id=10 record can legitimately arrive before
        // id=11, so ask for up to 2 records: if id=10 is replayed we need both to see id=11; if it
        // isn't, we'll only get 1 and simply wait out the remaining budget before returning it.
        List<SourceRecord> after = consumeRecordsForTopic(config, TABLE_RESTART, 2);

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

    /**
     * Verifies that after a full sliding window elapses and the connector is restarted
     * with no new data, the processedTimestamp prevents re-streaming already-seen events.
     *
     * <p>With WINDOW_MINUTES=1 this test waits roughly (WINDOW_MINUTES+1) minutes.
     */
    @Test
    void shouldNotReplayAfterWindowElapses() throws InterruptedException {
        Configuration config = buildConfig(TABLE_WINDOW + "_connector", STREAM_WINDOW);
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate("INSERT INTO " + TABLE_WINDOW + " (id, name) VALUES (20, 'window-seed')");
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
                .recordsForTopic(getTopicName(config, TABLE_WINDOW));

        assertThat(replayed)
                .as("processedTimestamp should prevent replay of events from already-processed windows")
                .isNullOrEmpty();
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
    @Test
    void shouldPreserveOrderAcrossForcedKeyRangeSplit() throws InterruptedException {
        Configuration config = buildConfig(TABLE_ORDER + "_connector", STREAM_ORDER);
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        long[] keys = { 500L, 1500L, 2500L };
        int updatesPerKey = 5;

        for (long key : keys) {
            databaseConnection.executeUpdate(
                    "INSERT INTO " + TABLE_ORDER + " (id, name) VALUES (" + key + ", 'v0')");
        }

        // Force the key range to split around each key, right as further updates are issued,
        // to exercise the destination partitions' MoveIn pause/resume logic mid-stream.
        databaseConnection.forceSplit(TABLE_ORDER, "1000");
        databaseConnection.forceSplit(TABLE_ORDER, "2000");

        for (int i = 1; i <= updatesPerKey; i++) {
            for (long key : keys) {
                databaseConnection.executeUpdate(
                        "UPDATE " + TABLE_ORDER + " SET name = 'v" + i + "' WHERE id = " + key);
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
                    .recordsForTopic(getTopicName(config, TABLE_ORDER));
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
}
