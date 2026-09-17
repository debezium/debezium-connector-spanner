/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assumptions;
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
 * Parameterized across all partition modes. Verifies the actual content of change-stream
 * records matches documented behavior for various row/column scenarios.
 *
 * <p>This test is {@link RealSpannerCompatible}: when {@code -Dspanner.test.real=true} is
 * passed it runs against a real Cloud Spanner instance; otherwise it runs against the local
 * emulator.
 */
@RealSpannerCompatible
public class ChangeStreamCorrectContentIT extends AbstractSpannerConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamCorrectContentIT.class);

    private static final String compositePkTableName = "embedded_composite_pk_table";
    private static final String compositePkChangeStreamName = "embeddedCompositePkChangeStream";

    private static final String deleteEventTableName = "embedded_delete_event_table";
    private static final String deleteEventChangeStreamName = "embeddedDeleteEventChangeStream";

    private static final String nullTransitionTableName = "embedded_null_transition_table";
    private static final String nullTransitionChangeStreamName = "embeddedNullTransitionChangeStream";

    private static final String partialUpdateTableName = "embedded_partial_update_table";
    private static final String partialUpdateChangeStreamName = "embeddedPartialUpdateChangeStream";

    private static final String schemaEvolutionTableName = "embedded_schema_evolution_table";
    private static final String schemaEvolutionChangeStreamName = "embeddedSchemaEvolutionChangeStream";

    private static final String txnMetadataTableName = "embedded_txn_metadata_table";
    private static final String txnMetadataChangeStreamName = "embeddedTxnMetadataChangeStream";

    @BeforeEach
    void clearTopics() {
        clearKafkaTopics();
    }

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldCarryAllPrimaryKeyColumnsInKeyStruct(PartitionMode partitionMode, Dialect dialect) throws Exception {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(compositePkTableName, partitionMode, dialect);
        String stream = streamFor(compositePkChangeStreamName, partitionMode, dialect);

        String tableParams = "(tenant_id INT64, id INT64, value STRING(100)) PRIMARY KEY (tenant_id, id)";
        createTableWithCustomParamsAndStream(connection, partitionMode, table, tableParams, stream);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // Two rows share the same 'id' but differ by 'tenant_id'. If the connector
            // only keyed on 'id', these would incorrectly collide onto the same Kafka key.
            connection.executeUpdate(
                    "INSERT INTO " + table + "(tenant_id, id, value) VALUES (1, 100, 'Tenant1Row')");
            connection.executeUpdate(
                    "INSERT INTO " + table + "(tenant_id, id, value) VALUES (2, 100, 'Tenant2Row')");
            connection.executeUpdate(
                    "UPDATE " + table + " SET value = 'Tenant1RowUpdated' WHERE tenant_id = 1 AND id = 100");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(3);

            SourceRecord tenant1Insert = records.get(0);
            SourceRecord tenant2Insert = records.get(1);
            SourceRecord tenant1Update = records.get(2);

            // Key schema field order must match the column's declared ordinal position:
            // tenant_id (1) then id (2).
            List<Field> keyFields = tenant1Insert.keySchema().fields();
            assertThat(keyFields).hasSize(2);
            assertThat(keyFields.get(0).name()).isEqualTo("tenant_id");
            assertThat(keyFields.get(1).name()).isEqualTo("id");

            Struct tenant1Key = (Struct) tenant1Insert.key();
            assertThat(tenant1Key.getInt64("tenant_id")).isEqualTo(1L);
            assertThat(tenant1Key.getInt64("id")).isEqualTo(100L);

            Struct tenant2Key = (Struct) tenant2Insert.key();
            assertThat(tenant2Key.getInt64("tenant_id")).isEqualTo(2L);
            assertThat(tenant2Key.getInt64("id")).isEqualTo(100L);

            // Same 'id', different 'tenant_id': the two rows must not resolve to the same key.
            assertThat(tenant1Key).isNotEqualTo(tenant2Key);

            // The update must be keyed to tenant 1's row only, and must not affect tenant 2's data.
            Struct tenant1UpdateKey = (Struct) tenant1Update.key();
            assertThat(tenant1UpdateKey).isEqualTo(tenant1Key);

            Struct tenant1After = ((Struct) tenant1Update.value()).getStruct("after");
            assertThat(tenant1After.getString("value")).isEqualTo("Tenant1RowUpdated");

            Struct tenant2After = ((Struct) tenant2Insert.value()).getStruct("after");
            assertThat(tenant2After.getString("value")).isEqualTo("Tenant2Row");

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldCarryLastKnownValuesInBeforeOnDelete(PartitionMode partitionMode, Dialect dialect) throws Exception {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(deleteEventTableName, partitionMode, dialect);
        String stream = streamFor(deleteEventChangeStreamName, partitionMode, dialect);

        String tableParams = "(id INT64, value STRING(100), status STRING(20), score INT64) PRIMARY KEY (id)";
        createTableWithCustomParamsAndStream(connection, partitionMode, table, tableParams, stream);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, value, status, score) VALUES (1, 'Alice', 'active', 10)");
            // Update before deleting, so the delete's "before" must reflect this updated
            // state, not the original insert values.
            connection.executeUpdate(
                    "UPDATE " + table + " SET status = 'inactive' WHERE id = 1");
            connection.executeUpdate(
                    "DELETE FROM " + table + " WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            // insert + update + delete + tombstone
            assertThat(records).hasSize(4);

            Struct insertRecord = (Struct) records.get(0).value();
            assertThat(insertRecord.get("op")).isEqualTo("c");

            Struct updateRecord = (Struct) records.get(1).value();
            assertThat(updateRecord.get("op")).isEqualTo("u");

            Struct deleteRecord = (Struct) records.get(2).value();
            assertThat(deleteRecord.get("op")).isEqualTo("d");

            Struct before = deleteRecord.getStruct("before");
            assertThat(before).isNotNull();
            assertThat(before.getInt64("id")).isEqualTo(1L);
            assertThat(before.getString("value")).isEqualTo("Alice");
            // Must reflect the updated status, not the original insert value.
            assertThat(before.getString("status")).isEqualTo("inactive");
            assertThat(before.getInt64("score")).isEqualTo(10);

            assertThat(deleteRecord.getStruct("after")).isNull();

            // Tombstone: same key, completely null value.
            assertThat(records.get(3).value()).isNull();

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldRoundTripNullColumnTransitions(PartitionMode partitionMode, Dialect dialect) throws Exception {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(nullTransitionTableName, partitionMode, dialect);
        String stream = streamFor(nullTransitionChangeStreamName, partitionMode, dialect);

        String tableParams = "(id INT64, nickname STRING(100)) PRIMARY KEY (id)";
        connection.createTable(table, tableParams);
        connection.createChangeStream(stream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, nickname) VALUES (1, NULL)");
            connection.executeUpdate(
                    "UPDATE " + table + " SET nickname = 'Ace' WHERE id = 1");
            connection.executeUpdate(
                    "UPDATE " + table + " SET nickname = NULL WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(3);

            Struct insertRecord = (Struct) records.get(0).value();
            assertThat(insertRecord.get("op")).isEqualTo("c");
            Struct insertAfter = insertRecord.getStruct("after");
            assertThat(insertAfter.getString("nickname")).isNull();

            Struct setRecord = (Struct) records.get(1).value();
            assertThat(setRecord.get("op")).isEqualTo("u");
            Struct setBefore = setRecord.getStruct("before");
            Struct setAfter = setRecord.getStruct("after");
            assertThat(setBefore.getString("nickname")).isNull();
            assertThat(setAfter.getString("nickname")).isEqualTo("Ace");

            Struct clearRecord = (Struct) records.get(2).value();
            assertThat(clearRecord.get("op")).isEqualTo("u");
            Struct clearBefore = clearRecord.getStruct("before");
            Struct clearAfter = clearRecord.getStruct("after");
            // Must distinguish "value went back to null" from "field left unset".
            assertThat(clearBefore.getString("nickname")).isEqualTo("Ace");
            assertThat(clearAfter.schema().field("nickname")).isNotNull();
            assertThat(clearAfter.getString("nickname")).isNull();

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldCarryUnchangedColumnsThroughOnPartialUpdate(PartitionMode partitionMode, Dialect dialect) throws Exception {
        Assumptions.assumeTrue(!Connection.isRealSpanner(),
                "Skipping: on real Spanner, an UPDATE's payload omits columns that weren't part of its SET "
                        + "clause, so an unchanged column comes back null instead of its real value. See the "
                        + "class Javadoc / doc/change-stream-integration-tests.md for the root cause.");

        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(partialUpdateTableName, partitionMode, dialect);
        String stream = streamFor(partialUpdateChangeStreamName, partitionMode, dialect);

        String tableParams = "(id INT64, value STRING(100), status STRING(20), score INT64) PRIMARY KEY (id)";
        createTableWithCustomParamsAndStream(connection, partitionMode, table, tableParams, stream);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, value, status, score) VALUES (1, 'Alice', 'active', 10)");
            // Only 'score' is touched here — 'value' and 'status' are left alone.
            connection.executeUpdate(
                    "UPDATE " + table + " SET score = 20 WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(2);

            Struct updateRecord = (Struct) records.get(1).value();
            assertThat(updateRecord.get("op")).isEqualTo("u");

            Struct before = updateRecord.getStruct("before");
            Struct after = updateRecord.getStruct("after");

            // Untouched columns must still resolve correctly on both sides.
            assertThat(before.getString("value")).isEqualTo("Alice");
            assertThat(before.getString("status")).isEqualTo("active");
            assertThat(after.getString("value")).isEqualTo("Alice");
            assertThat(after.getString("status")).isEqualTo("active");

            // The touched column must show the actual old -> new transition.
            assertThat(before.getInt64("score")).isEqualTo(10);
            assertThat(after.getInt64("score")).isEqualTo(20);

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldPickUpColumnAddedAfterStreamCreationWithoutReconfiguring(PartitionMode partitionMode, Dialect dialect) throws Exception {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(schemaEvolutionTableName, partitionMode, dialect);
        String stream = streamFor(schemaEvolutionChangeStreamName, partitionMode, dialect);

        createTableAndStream(connection, partitionMode, table, stream);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, value) VALUES (1, 'Alice')");

            // Schema change happens mid-stream, without touching the change stream's own
            // configuration. Per the docs, a whole-table stream should pick this up
            // automatically.
            connection.updateTable(table, " ADD COLUMN age INT64");

            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, value, age) VALUES (2, 'Bob', 30)");
            connection.executeUpdate(
                    "UPDATE " + table + " SET age = 99 WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(3);

            Struct preAlterInsert = (Struct) records.get(0).value();
            assertThat(preAlterInsert.get("op")).isEqualTo("c");
            Struct preAlterAfter = preAlterInsert.getStruct("after");
            assertThat(preAlterAfter.getString("value")).isEqualTo("Alice");

            Struct postAlterInsert = (Struct) records.get(1).value();
            assertThat(postAlterInsert.get("op")).isEqualTo("c");
            Struct postAlterAfter = postAlterInsert.getStruct("after");
            assertThat(postAlterAfter.getString("value")).isEqualTo("Bob");
            assertThat(postAlterAfter.getInt64("age")).isEqualTo(30L);

            // Existing row, updated after the column was added: the new column must be
            // usable without restarting or reconfiguring the connector.
            Struct backfillUpdate = (Struct) records.get(2).value();
            assertThat(backfillUpdate.get("op")).isEqualTo("u");
            Struct backfillAfter = backfillUpdate.getStruct("after");
            assertThat(backfillAfter.getInt64("age")).isEqualTo(99L);

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldDefaultTransactionTagAndSystemTransactionFlagForOrdinaryWrites(PartitionMode partitionMode, Dialect dialect) throws Exception {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(txnMetadataTableName, partitionMode, dialect);
        String stream = streamFor(txnMetadataChangeStreamName, partitionMode, dialect);

        createTableAndStream(connection, partitionMode, table, stream);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, value) VALUES (1, 'Untagged')");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(1);

            Struct source = ((Struct) records.get(0).value()).getStruct("source");
            assertThat(source.getString("transaction_tag")).isEmpty();
            assertThat(source.getBoolean("system_transaction")).isFalse();

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    // The local Docker emulator does not propagate transaction tags through its change stream.
    // Runs against real Spanner (-Dspanner.test.real=true).
    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldSurfaceExplicitTransactionTag(PartitionMode partitionMode, Dialect dialect) throws Exception {
        Assumptions.assumeTrue(Connection.isRealSpanner(),
                "Transaction tags are not propagated by the local Docker emulator. Run against real Spanner "
                        + "instead (-Dspanner.test.real=true)");
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(txnMetadataTableName, partitionMode, dialect);
        String stream = streamFor(txnMetadataChangeStreamName, partitionMode, dialect);

        createTableAndStream(connection, partitionMode, table, stream);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.databaseClient.readWriteTransaction(Options.tag("test-transaction-tag"))
                    .run(transaction -> transaction.executeUpdate(
                            Statement.of("INSERT INTO " + table + "(id, value) VALUES (2, 'Tagged')")));

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(1);

            Struct source = ((Struct) records.get(0).value()).getStruct("source");
            assertThat(source.getString("transaction_tag")).isEqualTo("test-transaction-tag");

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }
}
