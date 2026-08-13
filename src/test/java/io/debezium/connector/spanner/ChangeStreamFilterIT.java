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
import org.junit.jupiter.params.provider.EnumSource;

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
    @EnumSource(PartitionMode.class)
    public void shouldExcludeDeleteEventsAndTheirTombstones(PartitionMode partitionMode) throws InterruptedException, ExecutionException {
        String tableNameExcludeDelete = tableNameExcludeDeletePrefix + "_" + partitionMode.name().toLowerCase();
        String changeStreamNameExcludeDelete = changeStreamNameExcludeDeletePrefix + partitionMode.name();
        databaseConnection.createTable(tableNameExcludeDelete + "(id INT64, name STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamExcludeDelete(changeStreamNameExcludeDelete, partitionMode, tableNameExcludeDelete);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStreamNameExcludeDelete, tableNameExcludeDelete, partitionMode);

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableNameExcludeDelete + "(id, name) VALUES (1, 'Alice')");
            databaseConnection.executeUpdate(
                    "UPDATE " + tableNameExcludeDelete + " SET name = 'Bob' WHERE id = 1");
            databaseConnection.executeUpdate(
                    "DELETE FROM " + tableNameExcludeDelete + " WHERE id = 1");

            // A second row, inserted and never touched again, gives us a clear signal
            // that the connector is still alive and delivering records after the
            // excluded delete - not just coincidentally quiet.
            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableNameExcludeDelete + "(id, name) VALUES (2, 'Carol')");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableNameExcludeDelete));

            // insert(1) + update(1) + insert(2) - no delete, and therefore no tombstone.
            assertThat(records).hasSize(3);

            assertThat(((Struct) records.get(0).value()).get("op")).isEqualTo("c");
            assertThat(((Struct) records.get(1).value()).get("op")).isEqualTo("u");

            Struct secondInsert = (Struct) records.get(2).value();
            assertThat(secondInsert.get("op")).isEqualTo("c");
            assertThat(secondInsert.getStruct("after").getString("name")).isEqualTo("Carol");
        }
        finally {
            databaseConnection.dropChangeStream(changeStreamNameExcludeDelete);
            databaseConnection.dropTable(tableNameExcludeDelete);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldReflectRealPriorStateOnUpdateAfterAnExcludedInsert(PartitionMode partitionMode) throws InterruptedException, ExecutionException {
        String tableNameExcludeInsert = tableNameExcludeInsertPrefix + "_" + partitionMode.name().toLowerCase();
        String changeStreamNameExcludeInsert = changeStreamNameExcludeInsertPrefix + partitionMode.name();
        databaseConnection.createTable(tableNameExcludeInsert + "(id INT64, name STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamExcludeInsert(changeStreamNameExcludeInsert, partitionMode, tableNameExcludeInsert);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStreamNameExcludeInsert, tableNameExcludeInsert, partitionMode);

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // This insert is invisible to the stream, but the row genuinely exists
            // afterward with these values.
            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableNameExcludeInsert + "(id, name) VALUES (1, 'Alice')");
            databaseConnection.executeUpdate(
                    "UPDATE " + tableNameExcludeInsert + " SET name = 'Bob' WHERE id = 1");
            databaseConnection.executeUpdate(
                    "DELETE FROM " + tableNameExcludeInsert + " WHERE id = 1");

            // A row that's only ever inserted, never revisited - it must produce zero
            // records at all, not just a suppressed-but-otherwise-present one.
            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableNameExcludeInsert + "(id, name) VALUES (2, 'Carol')");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableNameExcludeInsert));

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
            databaseConnection.dropChangeStream(changeStreamNameExcludeInsert);
            databaseConnection.dropTable(tableNameExcludeInsert);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldExcludeUpdateEventsButReflectRealStateOnSubsequentDelete(PartitionMode partitionMode) throws InterruptedException, ExecutionException {
        String tableNameExcludeUpdate = tableNameExcludeUpdatePrefix + "_" + partitionMode.name().toLowerCase();
        String changeStreamNameExcludeUpdate = changeStreamNameExcludeUpdatePrefix + partitionMode.name();
        databaseConnection.createTable(tableNameExcludeUpdate + "(id INT64, name STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamExcludeUpdate(changeStreamNameExcludeUpdate, partitionMode, tableNameExcludeUpdate);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStreamNameExcludeUpdate, tableNameExcludeUpdate, partitionMode);

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableNameExcludeUpdate + "(id, name) VALUES (1, 'Alice')");
            // This update really executes against the row - it's excluded from the
            // change stream, not from the database itself.
            databaseConnection.executeUpdate(
                    "UPDATE " + tableNameExcludeUpdate + " SET name = 'Bob' WHERE id = 1");
            databaseConnection.executeUpdate(
                    "DELETE FROM " + tableNameExcludeUpdate + " WHERE id = 1");

            // A second row, inserted and never touched again, gives us a clear signal
            // that the connector is still alive and delivering records after the
            // excluded update - not just coincidentally quiet.
            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableNameExcludeUpdate + "(id, name) VALUES (2, 'Carol')");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableNameExcludeUpdate));

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
            databaseConnection.dropChangeStream(changeStreamNameExcludeUpdate);
            databaseConnection.dropTable(tableNameExcludeUpdate);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldNotRecordTransactionExplicitlyExcludedFromChangeStreams(PartitionMode partitionMode) throws InterruptedException, ExecutionException {
        String tableNameTxnExclusion = tableNameTxnExclusionPrefix + "_" + partitionMode.name().toLowerCase();
        String changeStreamNameTxnExclusion = changeStreamNameTxnExclusionPrefix + partitionMode.name();
        databaseConnection.createTable(tableNameTxnExclusion + "(id INT64, name STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamAllowTxnExclusion(changeStreamNameTxnExclusion, partitionMode, tableNameTxnExclusion);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStreamNameTxnExclusion, tableNameTxnExclusion, partitionMode);

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableNameTxnExclusion + "(id, name) VALUES (1, 'Alice')");

            // This transaction really executes against the row - it's excluded from the
            // change stream, not from the database itself.
            databaseConnection.databaseClient.readWriteTransaction(Options.excludeTxnFromChangeStreams())
                    .run(transaction -> transaction.executeUpdate(
                            Statement.of("UPDATE " + tableNameTxnExclusion + " SET name = 'Excluded' WHERE id = 1")));

            databaseConnection.executeUpdate(
                    "UPDATE " + tableNameTxnExclusion + " SET name = 'Bob' WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableNameTxnExclusion));

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
            databaseConnection.dropChangeStream(changeStreamNameTxnExclusion);
            databaseConnection.dropTable(tableNameTxnExclusion);
        }
    }
}
