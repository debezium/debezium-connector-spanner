/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Statement;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * Parameterized across both partition modes; each test creates and drops its own
 * partition-mode-suffixed table/change stream per invocation.
 *
 * <p>This test is {@link RealSpannerCompatible}: when {@code -Dspanner.test.real=true} is
 * passed it runs against a real Cloud Spanner instance; otherwise it runs against the local
 * emulator.
 */
@RealSpannerCompatible
public class ChangeStreamFilterIT extends AbstractSpannerConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamFilterIT.class);

    private static final String tableNameExcludeDeletePrefix = "embedded_exclude_delete_table";
    private static final String changeStreamNameExcludeDeletePrefix = "embeddedExcludeDeleteStream";

    private static final String tableNameExcludeInsertPrefix = "embedded_exclude_insert_table";
    private static final String changeStreamNameExcludeInsertPrefix = "embeddedExcludeInsertStream";

    private static final String tableNameExcludeUpdatePrefix = "embedded_exclude_update_table";
    private static final String changeStreamNameExcludeUpdatePrefix = "embeddedExcludeUpdateStream";

    private static final String tableNameTxnExclusionPrefix = "embedded_txn_exclusion_table";
    private static final String changeStreamNameTxnExclusionPrefix = "embeddedTxnExclusionStream";

    @BeforeEach
    void initFramework() {
        clearKafkaTopics();
        initializeConnectorTestFramework();
    }

    @AfterEach
    void ensureConnectorStopped() throws InterruptedException {
        stopConnector();
        assertConnectorNotRunning();
    }

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldExcludeDeleteEventsAndTheirTombstones(PartitionMode partitionMode, Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(tableNameExcludeDeletePrefix, partitionMode, dialect);
        String stream = streamFor(changeStreamNameExcludeDeletePrefix, partitionMode, dialect);

        String tableParams = "(id INT64, name STRING(100)) PRIMARY KEY (id)";
        connection.createTable(table, tableParams);
        connection.createChangeStreamExcludeDelete(stream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, name) VALUES (1, 'Alice')");
            connection.executeUpdate(
                    "UPDATE " + table + " SET name = 'Bob' WHERE id = 1");
            connection.executeUpdate(
                    "DELETE FROM " + table + " WHERE id = 1");

            // A second row, inserted and never touched again, gives us a clear signal
            // that the connector is still alive and delivering records after the
            // excluded delete - not just coincidentally quiet.
            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, name) VALUES (2, 'Carol')");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));

            // insert(1) + update(1) + insert(2) - no delete, and therefore no tombstone.
            assertThat(records).hasSize(3);

            assertThat(((Struct) records.get(0).value()).get("op")).isEqualTo("c");
            assertThat(((Struct) records.get(1).value()).get("op")).isEqualTo("u");

            Struct secondInsert = (Struct) records.get(2).value();
            assertThat(secondInsert.get("op")).isEqualTo("c");
            assertThat(secondInsert.getStruct("after").getString("name")).isEqualTo("Carol");
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldReflectRealPriorStateOnUpdateAfterAnExcludedInsert(PartitionMode partitionMode, Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(tableNameExcludeInsertPrefix, partitionMode, dialect);
        String stream = streamFor(changeStreamNameExcludeInsertPrefix, partitionMode, dialect);

        String tableParams = "(id INT64, name STRING(100)) PRIMARY KEY (id)";
        connection.createTable(table, tableParams);
        connection.createChangeStreamExcludeInsert(stream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // This insert is invisible to the stream, but the row genuinely exists
            // afterward with these values.
            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, name) VALUES (1, 'Alice')");
            connection.executeUpdate(
                    "UPDATE " + table + " SET name = 'Bob' WHERE id = 1");
            connection.executeUpdate(
                    "DELETE FROM " + table + " WHERE id = 1");

            // A row that's only ever inserted, never revisited - it must produce zero
            // records at all, not just a suppressed-but-otherwise-present one.
            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, name) VALUES (2, 'Carol')");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));

            // update(1) + delete(1) + tombstone(1) - no insert for either row.
            assertThat(records).hasSize(3);

            Struct updateRecord = (Struct) records.get(0).value();
            assertThat(updateRecord.get("op")).isEqualTo("u");
            // The row's real prior value was 'Alice', even though the stream never
            // reported the insert that created it.
            assertThat(updateRecord.getStruct("before").getString("name")).isEqualTo("Alice");
            assertThat(updateRecord.getStruct("after").getString("name")).isEqualTo("Bob");

            Struct deleteRecord = (Struct) records.get(1).value();
            assertThat(deleteRecord.get("op")).isEqualTo("d");
            assertThat(deleteRecord.getStruct("before").getString("name")).isEqualTo("Bob");

            // Tombstone for row 1's delete.
            assertThat(records.get(2).value()).isNull();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldExcludeUpdateEventsButReflectRealStateOnSubsequentDelete(PartitionMode partitionMode, Dialect dialect)
            throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(tableNameExcludeUpdatePrefix, partitionMode, dialect);
        String stream = streamFor(changeStreamNameExcludeUpdatePrefix, partitionMode, dialect);

        String tableParams = "(id INT64, name STRING(100)) PRIMARY KEY (id)";
        connection.createTable(table, tableParams);
        connection.createChangeStreamExcludeUpdate(stream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, name) VALUES (1, 'Alice')");
            // This update really executes against the row - it's excluded from the
            // change stream, not from the database itself.
            connection.executeUpdate(
                    "UPDATE " + table + " SET name = 'Bob' WHERE id = 1");
            connection.executeUpdate(
                    "DELETE FROM " + table + " WHERE id = 1");

            // A second row, inserted and never touched again, gives us a clear signal
            // that the connector is still alive and delivering records after the
            // excluded update - not just coincidentally quiet.
            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, name) VALUES (2, 'Carol')");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));

            // insert(1) + delete(1) + tombstone(1) + insert(2) - no update event at all.
            assertThat(records).hasSize(4);

            Struct insertRecord = (Struct) records.get(0).value();
            assertThat(insertRecord.get("op")).isEqualTo("c");
            assertThat(insertRecord.getStruct("after").getString("name")).isEqualTo("Alice");

            Struct deleteRecord = (Struct) records.get(1).value();
            assertThat(deleteRecord.get("op")).isEqualTo("d");
            // The row's real data did change to 'Bob' before being deleted - the stream
            // just never reported the update itself.
            assertThat(deleteRecord.getStruct("before").getString("name")).isEqualTo("Bob");

            // Tombstone for row 1's delete.
            assertThat(records.get(2).value()).isNull();

            Struct secondInsert = (Struct) records.get(3).value();
            assertThat(secondInsert.get("op")).isEqualTo("c");
            assertThat(secondInsert.getStruct("after").getString("name")).isEqualTo("Carol");
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldNotRecordTransactionExplicitlyExcludedFromChangeStreams(PartitionMode partitionMode, Dialect dialect)
            throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(tableNameTxnExclusionPrefix, partitionMode, dialect);
        String stream = streamFor(changeStreamNameTxnExclusionPrefix, partitionMode, dialect);

        String tableParams = "(id INT64, name STRING(100)) PRIMARY KEY (id)";
        connection.createTable(table, tableParams);
        connection.createChangeStreamAllowTxnExclusion(stream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, name) VALUES (1, 'Alice')");

            // This transaction really executes against the row - it's excluded from the
            // change stream, not from the database itself.
            connection.databaseClient.readWriteTransaction(Options.excludeTxnFromChangeStreams())
                    .run(transaction -> transaction.executeUpdate(
                            Statement.of("UPDATE " + table + " SET name = 'Excluded' WHERE id = 1")));

            connection.executeUpdate(
                    "UPDATE " + table + " SET name = 'Bob' WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));

            // insert + the final visible update - the excluded transaction produces
            // no record of its own at all.
            assertThat(records).hasSize(2);

            Struct insertRecord = (Struct) records.get(0).value();
            assertThat(insertRecord.get("op")).isEqualTo("c");
            assertThat(insertRecord.getStruct("after").getString("name")).isEqualTo("Alice");

            Struct visibleUpdate = (Struct) records.get(1).value();
            assertThat(visibleUpdate.get("op")).isEqualTo("u");
            // The row's real data did change to 'Excluded' - the stream just never
            // reported it, so this visible update's "before" reflects that real prior
            // state, not the last value the stream actually showed.
            assertThat(visibleUpdate.getStruct("before").getString("name")).isEqualTo("Excluded");
            assertThat(visibleUpdate.getStruct("after").getString("name")).isEqualTo("Bob");
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }
}
