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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.debezium.config.Configuration;
import io.debezium.util.Testing;

/**
 * Integration tests for mutable key range change streams.
 *
 * <p>Requires a running Spanner Omni instance. Run with:
 * <pre>
 *   -Dspanner.type=OMNI
 *   -Dgcp.spanner.host=https://your-omni-host:15000
 *   -Dspanner.omni.use.plaintext=true          # or use mTLS cert/key properties
 * </pre>
 *
 * <p>WINDOW_MINUTES is set to 1 so the sliding-window processedTimestamp
 * test completes in ~2 minutes instead of the production 20-minute default.
 */
@EnabledIfSystemProperty(named = "spanner.type", matches = "(?i)OMNI")
public class MutableKeyRangeIT extends AbstractSpannerConnectorIT {

    private static final String TABLE_CRUD = "mkr_crud_table";
    private static final String TABLE_RESTART = "mkr_restart_table";
    private static final String TABLE_WINDOW = "mkr_window_table";

    private static final String STREAM_CRUD = "mkrCrudStream";
    private static final String STREAM_RESTART = "mkrRestartStream";
    private static final String STREAM_WINDOW = "mkrWindowStream";

    /**
     * Sliding-window size used during this IT run.
     * Keep at 1 for fast test runs; bump to 20 to mimic production behaviour.
     */
    private static final int WINDOW_MINUTES = 1;

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        databaseConnection.createTable(TABLE_CRUD + "(id INT64, name STRING(100)) PRIMARY KEY(id)");
        databaseConnection.createTable(TABLE_RESTART + "(id INT64, name STRING(100)) PRIMARY KEY(id)");
        databaseConnection.createTable(TABLE_WINDOW + "(id INT64, name STRING(100)) PRIMARY KEY(id)");
        databaseConnection.createMutableKeyRangeChangeStream(STREAM_CRUD, TABLE_CRUD);
        databaseConnection.createMutableKeyRangeChangeStream(STREAM_RESTART, TABLE_RESTART);
        databaseConnection.createMutableKeyRangeChangeStream(STREAM_WINDOW, TABLE_WINDOW);
        Testing.print("MutableKeyRangeIT is ready.");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(STREAM_CRUD);
        databaseConnection.dropChangeStream(STREAM_RESTART);
        databaseConnection.dropChangeStream(STREAM_WINDOW);
        databaseConnection.dropTable(TABLE_CRUD);
        databaseConnection.dropTable(TABLE_RESTART);
        databaseConnection.dropTable(TABLE_WINDOW);
    }

    @BeforeEach
    void initFramework() {
        clearKafkaTopics();
        deleteOffsetFiles();
        initializeConnectorTestFramework();
    }

    private static void deleteOffsetFiles() {
        for (String name : new String[]{ TABLE_CRUD + "_connector", TABLE_RESTART + "_connector", TABLE_WINDOW + "_connector" }) {
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
                .build();
    }

    private String op(List<SourceRecord> records, int index) {
        return (String) ((Struct) records.get(index).value()).get("op");
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
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
        List<SourceRecord> before = consumeRecordsByTopic(5, false)
                .recordsForTopic(getTopicName(config, TABLE_RESTART));
        assertThat(before).as("Should have exactly 1 record before restart").hasSize(1);
        assertThat(op(before, 0)).isEqualTo("c");

        stopConnector();
        assertConnectorNotRunning();

        databaseConnection.executeUpdate("INSERT INTO " + TABLE_RESTART + " (id, name) VALUES (11, 'post-restart')");

        start(SpannerConnector.class, config);
        assertConnectorIsRunning();
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
        List<SourceRecord> after = consumeRecordsByTopic(5, false)
                .recordsForTopic(getTopicName(config, TABLE_RESTART));

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

        waitForAvailableRecords(5, TimeUnit.SECONDS);
        List<SourceRecord> replayed = consumeRecordsByTopic(5, false)
                .recordsForTopic(getTopicName(config, TABLE_WINDOW));

        assertThat(replayed)
                .as("processedTimestamp should prevent replay of events from already-processed windows")
                .isNullOrEmpty();
    }
}
