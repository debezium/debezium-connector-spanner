/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * This test is {@link RealSpannerCompatible}: when {@code -Dspanner.test.real=true} is
 * passed it runs against a real Cloud Spanner instance; otherwise it runs against the local
 * emulator.
 */
@RealSpannerCompatible
public class ChangeStreamOrderingAndTransactionalIT extends AbstractSpannerConnectorIT {

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

    private static final String multiTableTxnATableName = "embedded_txn_table_a";
    private static final String multiTableTxnBTableName = "embedded_txn_table_b";
    private static final String multiTableTxnChangeStreamName = "embeddedMultiTableTxnChangeStream";

    private static final String rapidUpdatesTableName = "embedded_rapid_updates_table";
    private static final String rapidUpdatesChangeStreamName = "embeddedRapidUpdatesChangeStream";
    private static final int NUMBER_OF_UPDATES = 8;

    private static final String restartContentTableName = "embedded_restart_content_table";
    private static final String restartContentChangeStreamName = "embeddedRestartContentChangeStream";

    @BeforeEach
    void clearTopics() {
        clearKafkaTopics();
    }

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldCorrelateChangesAcrossTablesInSameTransaction(PartitionMode partitionMode) throws Exception {
        String tableA = multiTableTxnATableName + "_" + partitionMode.name().toLowerCase();
        String tableB = multiTableTxnBTableName + "_" + partitionMode.name().toLowerCase();
        String changeStream = multiTableTxnChangeStreamName + partitionMode.name();
        databaseConnection.createTable(tableA + "(id INT64, value STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createTable(tableB + "(id INT64, value STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStream, partitionMode, tableA, tableB);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStream, tableA, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // Seed table A in its own transaction.
            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableA + "(id, value) VALUES (1, 'A-initial')");

            // One atomic transaction touching both tables.
            databaseConnection.executeUpdate(List.of(
                    "UPDATE " + tableA + " SET value = 'A-updated' WHERE id = 1",
                    "INSERT INTO " + tableB + "(id, value) VALUES (1, 'B-initial')"));

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(20, false);

            List<SourceRecord> tableARecords = sourceRecords.recordsForTopic(getTopicName(config, tableA));
            List<SourceRecord> tableBRecords = sourceRecords.recordsForTopic(getTopicName(config, tableB));
            assertThat(tableARecords).hasSize(2);
            assertThat(tableBRecords).hasSize(1);

            Struct seedInsert = (Struct) tableARecords.get(0).value();
            assertThat(seedInsert.get("op")).isEqualTo("c");
            String seedTxnId = seedInsert.getStruct("source").getString("server_transaction_id");

            Struct tableAUpdate = (Struct) tableARecords.get(1).value();
            assertThat(tableAUpdate.get("op")).isEqualTo("u");
            Struct tableAUpdateSource = tableAUpdate.getStruct("source");

            Struct tableBInsert = (Struct) tableBRecords.get(0).value();
            assertThat(tableBInsert.get("op")).isEqualTo("c");
            Struct tableBInsertSource = tableBInsert.getStruct("source");

            // The two changes from the atomic transaction must be correlated by the same
            // server transaction id and the same commit timestamp, even though they belong
            // to different tables.
            String sharedTxnId = tableAUpdateSource.getString("server_transaction_id");
            assertThat(tableBInsertSource.getString("server_transaction_id")).isEqualTo(sharedTxnId);
            assertThat(tableBInsertSource.getInt64("ts_ms")).isEqualTo(tableAUpdateSource.getInt64("ts_ms"));

            // The seed insert was a separate transaction and must not share the same id.
            assertThat(seedTxnId).isNotEqualTo(sharedTxnId);

            assertThat(tableAUpdate.getStruct("after").getString("value")).isEqualTo("A-updated");
            assertThat(tableBInsert.getStruct("after").getString("value")).isEqualTo("B-initial");

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            databaseConnection.dropChangeStream(changeStream);
            databaseConnection.dropTable(tableA);
            databaseConnection.dropTable(tableB);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldPreserveStrictOrderAcrossManyRapidUpdatesToSameRow(PartitionMode partitionMode) throws Exception {
        String table = rapidUpdatesTableName + "_" + partitionMode.name().toLowerCase();
        String changeStream = rapidUpdatesChangeStreamName + partitionMode.name();
        databaseConnection.createTable(table + "(id INT64, counter INT64) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + table + "(id, counter) VALUES (1, 0)");
            for (int i = 1; i <= NUMBER_OF_UPDATES; i++) {
                databaseConnection.executeUpdate(
                        "UPDATE " + table + " SET counter = " + i + " WHERE id = 1");
            }

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(30, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(NUMBER_OF_UPDATES + 1);

            assertThat(((Struct) records.get(0).value()).get("op")).isEqualTo("c");
            for (int i = 1; i <= NUMBER_OF_UPDATES; i++) {
                assertThat(((Struct) records.get(i).value()).get("op")).isEqualTo("u");
            }

            // Each record's "after.counter" must match the exact order the updates were
            // issued in - not just the op codes, but the actual content per step.
            List<Long> countersInOrder = new ArrayList<>();
            for (SourceRecord record : records) {
                countersInOrder.add(((Struct) record.value()).getStruct("after").getInt64("counter"));
            }
            List<Long> expected = new ArrayList<>();
            for (long i = 0; i <= NUMBER_OF_UPDATES; i++) {
                expected.add(i);
            }
            assertThat(countersInOrder).isEqualTo(expected);

            // Commit timestamps must be strictly increasing across the full sequence.
            long previousTimestamp = -1L;
            for (SourceRecord record : records) {
                long ts = ((Struct) record.value()).getStruct("source").getInt64("ts_ms");
                assertThat(ts).isGreaterThan(previousTimestamp);
                previousTimestamp = ts;
            }

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            databaseConnection.dropChangeStream(changeStream);
            databaseConnection.dropTable(table);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldResumeWithoutDuplicatingOrLosingContentAcrossRestart(PartitionMode partitionMode) throws Exception {
        Assumptions.assumeTrue(partitionMode == PartitionMode.IMMUTABLE_KEY_RANGE || Connection.isRealSpanner(),
                "Skipping: on the emulator, MUTABLE_KEY_RANGE doesn't redeliver the missed update once after "
                        + "restart - it redelivers the same content 5 times over about 40 seconds before Spanner "
                        + "rejects a query with OUT_OF_RANGE: Specified start_timestamp is too far in the future. "
                        + "Run with -Dspanner.test.real=true to exercise this mode.");
        String table = restartContentTableName + "_" + partitionMode.name().toLowerCase();
        String changeStream = restartContentChangeStreamName + partitionMode.name();
        databaseConnection.createTable(table + "(id INT64, name STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + table + "(id, name) VALUES (1, 'Alice')");

            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            SourceRecords beforeStop = consumeRecordsByTopic(10, false);
            List<SourceRecord> beforeStopRecords = beforeStop.recordsForTopic(getTopicName(config, table));
            assertThat(beforeStopRecords).hasSize(1);
            assertThat(((Struct) beforeStopRecords.get(0).value()).get("op")).isEqualTo("c");

            stopConnector();
            assertConnectorNotRunning();

            // This write happens entirely while the connector is down; it must be picked
            // up on restart, exactly once, with correct content - not lost, and not
            // duplicated alongside a replay of the pre-restart insert.
            databaseConnection.executeUpdate(
                    "UPDATE " + table + " SET name = 'Bob' WHERE id = 1");

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            SourceRecords afterRestart = consumeRecordsByTopic(10, false);
            List<SourceRecord> afterRestartRecords = afterRestart.recordsForTopic(getTopicName(config, table));
            assertThat(afterRestartRecords).hasSize(1);

            Struct missedUpdate = (Struct) afterRestartRecords.get(0).value();
            assertThat(missedUpdate.get("op")).isEqualTo("u");
            assertThat(missedUpdate.getStruct("before").getString("name")).isEqualTo("Alice");
            assertThat(missedUpdate.getStruct("after").getString("name")).isEqualTo("Bob");

            // The connector must also keep working correctly after resuming, not just
            // replay the backlog and then stall.
            databaseConnection.executeUpdate(
                    "UPDATE " + table + " SET name = 'Carol' WHERE id = 1");

            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            SourceRecords afterResume = consumeRecordsByTopic(10, false);
            List<SourceRecord> afterResumeRecords = afterResume.recordsForTopic(getTopicName(config, table));
            assertThat(afterResumeRecords).hasSize(1);

            Struct postRestartUpdate = (Struct) afterResumeRecords.get(0).value();
            assertThat(postRestartUpdate.get("op")).isEqualTo("u");
            assertThat(postRestartUpdate.getStruct("before").getString("name")).isEqualTo("Bob");
            assertThat(postRestartUpdate.getStruct("after").getString("name")).isEqualTo("Carol");

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            databaseConnection.dropChangeStream(changeStream);
            databaseConnection.dropTable(table);
        }
    }
}
