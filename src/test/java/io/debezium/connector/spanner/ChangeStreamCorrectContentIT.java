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
import org.junit.jupiter.params.provider.EnumSource;

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
    @EnumSource(PartitionMode.class)
    public void shouldCarryAllPrimaryKeyColumnsInKeyStruct(PartitionMode partitionMode) throws Exception {
        String table = compositePkTableName + "_" + partitionMode.name().toLowerCase();
        String changeStream = compositePkChangeStreamName + partitionMode.name();
        databaseConnection.createTable(table
                + "(tenant_id INT64, id INT64, name STRING(100)) PRIMARY KEY (tenant_id, id)");
        databaseConnection.createChangeStream(changeStream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // Two rows share the same 'id' but differ by 'tenant_id'. If the connector
            // only keyed on 'id', these would incorrectly collide onto the same Kafka key.
            databaseConnection.executeUpdate(
                    "INSERT INTO " + table + "(tenant_id, id, name) VALUES (1, 100, 'Tenant1Row')");
            databaseConnection.executeUpdate(
                    "INSERT INTO " + table + "(tenant_id, id, name) VALUES (2, 100, 'Tenant2Row')");
            databaseConnection.executeUpdate(
                    "UPDATE " + table + " SET name = 'Tenant1RowUpdated' WHERE tenant_id = 1 AND id = 100");

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
            assertThat(tenant1After.getString("name")).isEqualTo("Tenant1RowUpdated");

            Struct tenant2After = ((Struct) tenant2Insert.value()).getStruct("after");
            assertThat(tenant2After.getString("name")).isEqualTo("Tenant2Row");

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
    public void shouldCarryLastKnownValuesInBeforeOnDelete(PartitionMode partitionMode) throws Exception {
        String table = deleteEventTableName + "_" + partitionMode.name().toLowerCase();
        String changeStream = deleteEventChangeStreamName + partitionMode.name();
        databaseConnection.createTable(table
                + "(id INT64, name STRING(100), status STRING(20), score INT64) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + table + "(id, name, status, score) VALUES (1, 'Alice', 'active', 10)");
            // Update before deleting, so the delete's "before" must reflect this updated
            // state, not the original insert values.
            databaseConnection.executeUpdate(
                    "UPDATE " + table + " SET status = 'inactive' WHERE id = 1");
            databaseConnection.executeUpdate(
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
            assertThat(before.getString("name")).isEqualTo("Alice");
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
            databaseConnection.dropChangeStream(changeStream);
            databaseConnection.dropTable(table);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldRoundTripNullColumnTransitions(PartitionMode partitionMode) throws Exception {
        String table = nullTransitionTableName + "_" + partitionMode.name().toLowerCase();
        String changeStream = nullTransitionChangeStreamName + partitionMode.name();
        databaseConnection.createTable(table
                + "(id INT64, nickname STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + table + "(id, nickname) VALUES (1, NULL)");
            databaseConnection.executeUpdate(
                    "UPDATE " + table + " SET nickname = 'Ace' WHERE id = 1");
            databaseConnection.executeUpdate(
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
            databaseConnection.dropChangeStream(changeStream);
            databaseConnection.dropTable(table);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldCarryUnchangedColumnsThroughOnPartialUpdate(PartitionMode partitionMode) throws Exception {
        Assumptions.assumeTrue(!Connection.isRealSpanner(),
                "Skipping: on real Spanner, an UPDATE's payload omits columns that weren't part of its SET "
                        + "clause, so an unchanged column comes back null instead of its real value. See the "
                        + "class Javadoc / doc/change-stream-integration-tests.md for the root cause.");
        String table = partialUpdateTableName + "_" + partitionMode.name().toLowerCase();
        String changeStream = partialUpdateChangeStreamName + partitionMode.name();
        databaseConnection.createTable(table
                + "(id INT64, name STRING(100), status STRING(20), score INT64) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + table + "(id, name, status, score) VALUES (1, 'Alice', 'active', 10)");
            // Only 'score' is touched here — 'name' and 'status' are left alone.
            databaseConnection.executeUpdate(
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
            assertThat(before.getString("name")).isEqualTo("Alice");
            assertThat(before.getString("status")).isEqualTo("active");
            assertThat(after.getString("name")).isEqualTo("Alice");
            assertThat(after.getString("status")).isEqualTo("active");

            // The touched column must show the actual old -> new transition.
            assertThat(before.getInt64("score")).isEqualTo(10);
            assertThat(after.getInt64("score")).isEqualTo(20);

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
    public void shouldPickUpColumnAddedAfterStreamCreationWithoutReconfiguring(PartitionMode partitionMode) throws Exception {
        String table = schemaEvolutionTableName + "_" + partitionMode.name().toLowerCase();
        String changeStream = schemaEvolutionChangeStreamName + partitionMode.name();
        databaseConnection.createTable(table + "(id INT64, name STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + table + "(id, name) VALUES (1, 'Alice')");

            // Schema change happens mid-stream, without touching the change stream's own
            // configuration. Per the docs, a whole-table stream should pick this up
            // automatically.
            databaseConnection.updateDDL(List.of(
                    "ALTER TABLE " + table + " ADD COLUMN age INT64"));

            databaseConnection.executeUpdate(
                    "INSERT INTO " + table + "(id, name, age) VALUES (2, 'Bob', 30)");
            databaseConnection.executeUpdate(
                    "UPDATE " + table + " SET age = 99 WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(3);

            Struct preAlterInsert = (Struct) records.get(0).value();
            assertThat(preAlterInsert.get("op")).isEqualTo("c");
            Struct preAlterAfter = preAlterInsert.getStruct("after");
            assertThat(preAlterAfter.getString("name")).isEqualTo("Alice");

            Struct postAlterInsert = (Struct) records.get(1).value();
            assertThat(postAlterInsert.get("op")).isEqualTo("c");
            Struct postAlterAfter = postAlterInsert.getStruct("after");
            assertThat(postAlterAfter.getString("name")).isEqualTo("Bob");
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
            databaseConnection.dropChangeStream(changeStream);
            databaseConnection.dropTable(table);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldDefaultTransactionTagAndSystemTransactionFlagForOrdinaryWrites(PartitionMode partitionMode) throws Exception {
        String table = txnMetadataTableName + "_" + partitionMode.name().toLowerCase();
        String changeStream = txnMetadataChangeStreamName + partitionMode.name();
        databaseConnection.createTable(table + "(id INT64, value STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
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
            databaseConnection.dropChangeStream(changeStream);
            databaseConnection.dropTable(table);
        }
    }

    // The local Docker emulator does not propagate transaction tags through its change stream.
    // Runs against real Spanner (-Dspanner.test.real=true).
    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldSurfaceExplicitTransactionTag(PartitionMode partitionMode) throws Exception {
        Assumptions.assumeTrue(Connection.isRealSpanner(),
                "Transaction tags are not propagated by the local Docker emulator. Run against real Spanner "
                        + "instead (-Dspanner.test.real=true)");
        String table = txnMetadataTableName + "_tag_" + partitionMode.name().toLowerCase();
        String changeStream = txnMetadataChangeStreamName + "Tag" + partitionMode.name();
        databaseConnection.createTable(table + "(id INT64, value STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.databaseClient.readWriteTransaction(Options.tag("test-transaction-tag"))
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
            databaseConnection.dropChangeStream(changeStream);
            databaseConnection.dropTable(table);
        }
    }
}
