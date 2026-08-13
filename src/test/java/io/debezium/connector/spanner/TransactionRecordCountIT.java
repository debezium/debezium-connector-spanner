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
import org.junit.jupiter.api.Assumptions;
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
 *
 * <p>{@code MUTABLE_KEY_RANGE} self-skips unless running against real Spanner: connector
 * startup plus this test's DML can incidentally span the local emulator's ~15-20 second
 * background partition split, and a newly split {@code MUTABLE_KEY_RANGE} child partition
 * isn't picked up for streaming quickly enough - Spanner rejects the query with
 * {@code OUT_OF_RANGE: Specified start_timestamp is too far in the past}. Same root cause as
 * the one documented on {@link CrossPartitionSplitOrderingIT}; real Spanner splits based on
 * load rather than a fixed timer, so it isn't expected to hit this.
 */
@RealSpannerCompatible
public class TransactionRecordCountIT extends AbstractSpannerConnectorIT {

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

    private static final String tableNamePrefix = "embedded_txn_record_count_table";
    private static final String changeStreamNamePrefix = "embeddedTxnRecordCountStream";

    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldReportRecordAndPartitionCountsForTransaction(PartitionMode partitionMode) throws InterruptedException, ExecutionException {
        Assumptions.assumeTrue(partitionMode != PartitionMode.MUTABLE_KEY_RANGE || Connection.isRealSpanner(),
                "Skipping: this test incidentally spans the emulator's background partition split, and the "
                        + "connector doesn't pick up the new MUTABLE_KEY_RANGE child partition for streaming "
                        + "quickly enough - Spanner rejects the query with OUT_OF_RANGE: Specified start_timestamp "
                        + "is too far in the past. Run with -Dspanner.test.real=true to exercise this mode.");
        String tableName = tableNamePrefix + "_" + partitionMode.name().toLowerCase();
        String changeStreamName = changeStreamNamePrefix + partitionMode.name();
        databaseConnection.createTable(tableName + "(id INT64, value STRING(100)) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStreamName, partitionMode, tableName);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStreamName, tableName, partitionMode);

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // Seed row 1 in its own single-statement transaction.
            databaseConnection.executeUpdate(
                    "INSERT INTO " + tableName + "(id, value) VALUES (1, 'A-initial')");

            // One atomic transaction touching two rows - the transaction-wide record count
            // must reflect both changes, not just what one row's own record shows in
            // isolation.
            databaseConnection.executeUpdate(List.of(
                    "UPDATE " + tableName + " SET value = 'A-updated' WHERE id = 1",
                    "INSERT INTO " + tableName + "(id, value) VALUES (2, 'B-initial')"));

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
            assertThat(records).hasSize(3);

            Struct seedInsertSource = ((Struct) records.get(0).value()).getStruct("source");
            // A single-statement transaction touching one row: the transaction only ever
            // produced one record, entirely within one partition for this tiny table.
            assertThat(seedInsertSource.getInt64("number_records_in_transaction")).isEqualTo(1);
            assertThat(seedInsertSource.getInt64("number_of_partitions_in_transaction")).isEqualTo(1);

            Struct sharedUpdateSource = ((Struct) records.get(1).value()).getStruct("source");
            Struct sharedInsertSource = ((Struct) records.get(2).value()).getStruct("source");

            // Both records came from the same two-statement transaction: each one must
            // report the transaction-wide total (2), not a count scoped to itself or to
            // whichever partition happened to carry it.
            assertThat(sharedUpdateSource.getInt64("number_records_in_transaction")).isEqualTo(2);
            assertThat(sharedInsertSource.getInt64("number_records_in_transaction")).isEqualTo(2);

            // Both changes land in the same (only) partition for a table this small.
            assertThat(sharedUpdateSource.getInt64("number_of_partitions_in_transaction")).isEqualTo(1);
            assertThat(sharedInsertSource.getInt64("number_of_partitions_in_transaction")).isEqualTo(1);

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            databaseConnection.dropChangeStream(changeStreamName);
            databaseConnection.dropTable(tableName);
        }
    }
}
