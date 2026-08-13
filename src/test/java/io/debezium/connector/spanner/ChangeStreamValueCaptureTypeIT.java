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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
public class ChangeStreamValueCaptureTypeIT extends AbstractSpannerConnectorIT {

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

    private static final String tableNameNewValuesPrefix = "embedded_new_values_capture_table";
    private static final String changeStreamNameNewValuesPrefix = "embeddedNewValuesCaptureStream";

    private static final String tableNameNewRowPrefix = "embedded_new_row_capture_table";
    private static final String changeStreamNameNewRowPrefix = "embeddedNewRowCaptureStream";

    private static final String tableNameNewRowAndOldValuesPrefix = "embedded_new_row_old_values_capture_table";
    private static final String changeStreamNameNewRowAndOldValuesPrefix = "embeddedNewRowAndOldValuesCaptureStream";

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
    public void shouldCaptureFullNewRowWithNoNonKeyOldValues(PartitionMode partitionMode) throws InterruptedException, ExecutionException {
        Assumptions.assumeTrue(!Connection.isRealSpanner(),
                "Skipping: on real Cloud Spanner, NEW_VALUES's 'after' struct omits columns that "
                        + "weren't part of the UPDATE's SET clause, contrary to the emulator's full-row "
                        + "behavior - see doc/change-stream-integration-tests.md.");
        String tableName = tableNameNewValuesPrefix + "_" + partitionMode.name().toLowerCase();
        String changeStreamName = changeStreamNameNewValuesPrefix + partitionMode.name();
        databaseConnection.createTable(tableName
                + "(id INT64, name STRING(100), status STRING(20), score INT64) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamNewValue(changeStreamName, partitionMode, tableName);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStreamName, tableName, partitionMode);

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableName + "(id, name, status, score) VALUES (1, 'Alice', 'active', 10)");
            // Only 'score' is touched here — 'name' and 'status' are left alone.
            databaseConnection.executeUpdate(
                    "UPDATE " + tableName + " SET score = 20 WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
            assertThat(records).hasSize(2);

            Struct updateRecord = (Struct) records.get(1).value();
            assertThat(updateRecord.get("op")).isEqualTo("u");

            // NEW_VALUES captures no non-key old values; the primary key still identifies
            // the row, but none of the other columns' prior values are included.
            Struct before = updateRecord.getStruct("before");
            assertThat(before).isNotNull();
            assertThat(before.getInt64("id")).isEqualTo(1L);
            assertThat(before.getString("name")).isNull();
            assertThat(before.getString("status")).isNull();
            assertThat(before.getInt64("score")).isNull();

            // On the emulator, "after" always contains the full non-key row regardless of
            // which columns actually changed. This doesn't hold on real Cloud Spanner - see
            // the assumeTrue skip above.
            Struct after = updateRecord.getStruct("after");
            assertThat(after.getInt64("score")).isEqualTo(20);
            assertThat(after.getString("name")).isEqualTo("Alice");
            assertThat(after.getString("status")).isEqualTo("active");
        }
        finally {
            databaseConnection.dropChangeStream(changeStreamName);
            databaseConnection.dropTable(tableName);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldCaptureFullNewRowWithNoOldValues(PartitionMode partitionMode) throws InterruptedException, ExecutionException {
        String tableName = tableNameNewRowPrefix + "_" + partitionMode.name().toLowerCase();
        String changeStreamName = changeStreamNameNewRowPrefix + partitionMode.name();
        databaseConnection.createTable(tableName
                + "(id INT64, name STRING(100), status STRING(20), score INT64) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamNewRow(changeStreamName, partitionMode, tableName);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStreamName, tableName, partitionMode);

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableName + "(id, name, status, score) VALUES (1, 'Alice', 'active', 10)");
            // Only 'score' is touched here — 'name' and 'status' are left alone.
            databaseConnection.executeUpdate(
                    "UPDATE " + tableName + " SET score = 20 WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
            assertThat(records).hasSize(2);

            Struct updateRecord = (Struct) records.get(1).value();
            assertThat(updateRecord.get("op")).isEqualTo("u");

            // NEW_ROW captures no old values.
            Struct before = updateRecord.getStruct("before");
            assertThat(before).isNotNull();
            assertThat(before.getInt64("id")).isEqualTo(1L);
            assertThat(before.getString("name")).isNull();
            assertThat(before.getString("status")).isNull();
            assertThat(before.getInt64("score")).isNull();

            // NEW_ROW captures the full row - both modified and unmodified columns.
            Struct after = updateRecord.getStruct("after");
            assertThat(after.getInt64("score")).isEqualTo(20);
            assertThat(after.getString("name")).isEqualTo("Alice");
            assertThat(after.getString("status")).isEqualTo("active");
        }
        finally {
            databaseConnection.dropChangeStream(changeStreamName);
            databaseConnection.dropTable(tableName);
        }
    }

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldCaptureFullRowOnBothSides(PartitionMode partitionMode) throws InterruptedException, ExecutionException {
        Assumptions.assumeTrue(!Connection.isRealSpanner(),
                "Skipping: on real Cloud Spanner, NEW_ROW_AND_OLD_VALUES's 'before' struct omits "
                        + "columns that weren't part of the UPDATE's SET clause, contrary to the "
                        + "emulator's full-row behavior - see doc/change-stream-integration-tests.md.");
        String tableName = tableNameNewRowAndOldValuesPrefix + "_" + partitionMode.name().toLowerCase();
        String changeStreamName = changeStreamNameNewRowAndOldValuesPrefix + partitionMode.name();
        databaseConnection.createTable(tableName
                + "(id INT64, name STRING(100), status STRING(20), score INT64) PRIMARY KEY (id)");
        databaseConnection.createChangeStreamNewRowAndOldValues(changeStreamName, partitionMode, tableName);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStreamName, tableName, partitionMode);

            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableName + "(id, name, status, score) VALUES (1, 'Alice', 'active', 10)");
            // Only 'score' is touched here — 'name' and 'status' are left alone.
            databaseConnection.executeUpdate(
                    "UPDATE " + tableName + " SET score = 20 WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
            assertThat(records).hasSize(2);

            Struct updateRecord = (Struct) records.get(1).value();
            assertThat(updateRecord.get("op")).isEqualTo("u");

            Struct before = updateRecord.getStruct("before");
            assertThat(before.getInt64("score")).isEqualTo(10);
            assertThat(before.getString("name")).isEqualTo("Alice");
            assertThat(before.getString("status")).isEqualTo("active");

            Struct after = updateRecord.getStruct("after");
            assertThat(after.getInt64("score")).isEqualTo(20);
            assertThat(after.getString("name")).isEqualTo("Alice");
            assertThat(after.getString("status")).isEqualTo("active");
        }
        finally {
            databaseConnection.dropChangeStream(changeStreamName);
            databaseConnection.dropTable(tableName);
        }
    }
}
