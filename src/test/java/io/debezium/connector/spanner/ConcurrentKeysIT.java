/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * Parameterized across both partition modes.
 *
 * <p>This test is {@link RealSpannerCompatible}: when {@code -Dspanner.test.real=true} is
 * passed it runs against a real Cloud Spanner instance; otherwise it runs against the local
 * emulator.
 *
 * <p>{@code MUTABLE_KEY_RANGE} can't run against the emulator: this test runs long enough
 * (connector startup, task rebalancing, eight DML statements) to incidentally span the local
 * emulator's ~15-20 second background partition split, and a newly split
 * {@code MUTABLE_KEY_RANGE} child partition isn't picked up for streaming quickly enough -
 * Spanner rejects the query with
 * {@code OUT_OF_RANGE: Specified start_timestamp is too far in the past}. Same root cause as
 * the one documented on {@link CrossPartitionSplitOrderingIT}; real Spanner splits based on
 * load rather than a fixed timer, so it isn't expected to hit this.
 *
 * <p>The whole method is currently {@code @Disabled} regardless of backend - see its Javadoc.
 */
@RealSpannerCompatible
public class ConcurrentKeysIT extends AbstractSpannerConnectorIT {

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
        // bootstrap add latency the emulator doesn't have, so the debezium-embedded defaults
        // (30s wait for the first record, then up to 3 x 10s of additional polling) are sometimes
        // too tight here. Raise the defaults for real-Spanner runs so the suite is stable without
        // requiring extra -D flags on the command line.
        if (Connection.isRealSpanner()) {
            System.setProperty("debezium.test.records.waittime",
                    System.getProperty("debezium.test.records.waittime", "60"));
            System.setProperty("debezium.test.records.waittime.after.nulls",
                    System.getProperty("debezium.test.records.waittime.after.nulls", "5"));
        }
    }

    private static final String tableNamePrefix = "embedded_concurrent_keys_table";
    private static final String changeStreamNamePrefix = "embeddedConcurrentKeysStream";

    /**
     * Disabled on both backends.
     *
     * <p>Emulator: {@code MUTABLE_KEY_RANGE} can't run here - a background partition split
     * mid-test breaks streaming (see class Javadoc).
     *
     * <p>Real Spanner: UPDATE "before" images omit columns that weren't part of the
     * {@code SET} clause, because Spanner's change-stream {@code old_values} only includes
     * changed columns and the connector never backfills the rest. Confirmed on both partition
     * modes.
     *
     * <p>Re-enable once that real-Spanner bug is fixed.
     */
    @Disabled("Real-Spanner run: UPDATE 'before' images omit columns that weren't part of the "
            + "SET clause, since the connector never backfills unchanged columns missing from "
            + "Spanner's old_values. See Javadoc.")
    @ParameterizedTest
    @EnumSource(PartitionMode.class)
    public void shouldNotCrossContaminateStateBetweenInterleavedKeys(PartitionMode partitionMode) throws InterruptedException, ExecutionException {
        String tableName = tableNamePrefix + "_" + partitionMode.name().toLowerCase();
        String changeStreamName = changeStreamNamePrefix + partitionMode.name();
        databaseConnection.createTable(tableName
                + "(id INT64, name STRING(100), score INT64) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(changeStreamName, partitionMode, tableName);
        try {
            final Configuration config = buildTestConfig(baseConfig, changeStreamName, tableName, partitionMode);

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // Inserts and updates for four different keys are deliberately interleaved,
            // rather than done one key at a time, to stress any per-row state cache that
            // might be keyed or indexed incorrectly.
            databaseConnection.executeUpdate("INSERT INTO " + tableName + "(id, name, score) VALUES (1, 'Alice', 10)");
            databaseConnection.executeUpdate("INSERT INTO " + tableName + "(id, name, score) VALUES (2, 'Bob', 20)");
            databaseConnection.executeUpdate("UPDATE " + tableName + " SET score = 100 WHERE id = 1");
            databaseConnection.executeUpdate("INSERT INTO " + tableName + "(id, name, score) VALUES (3, 'Carol', 30)");
            databaseConnection.executeUpdate("UPDATE " + tableName + " SET score = 200 WHERE id = 2");
            databaseConnection.executeUpdate("UPDATE " + tableName + " SET score = 300 WHERE id = 3");
            databaseConnection.executeUpdate("INSERT INTO " + tableName + "(id, name, score) VALUES (4, 'Dave', 40)");
            databaseConnection.executeUpdate("UPDATE " + tableName + " SET score = 400 WHERE id = 4");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(30, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
            assertThat(records).hasSize(8);

            Map<Long, List<SourceRecord>> byKey = new LinkedHashMap<>();
            for (SourceRecord record : records) {
                long id = ((Struct) record.key()).getInt64("id");
                byKey.computeIfAbsent(id, k -> new ArrayList<>()).add(record);
            }
            assertThat(byKey.keySet()).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);

            String[] names = { "Alice", "Bob", "Carol", "Dave" };
            long[] initialScores = { 10, 20, 30, 40 };
            long[] updatedScores = { 100, 200, 300, 400 };

            for (int i = 0; i < 4; i++) {
                long id = i + 1L;
                List<SourceRecord> keyRecords = byKey.get(id);
                assertThat(keyRecords).hasSize(2);

                Struct insert = (Struct) keyRecords.get(0).value();
                assertThat(insert.get("op")).isEqualTo("c");
                Struct insertAfter = insert.getStruct("after");
                assertThat(insertAfter.getString("name")).isEqualTo(names[i]);
                assertThat(insertAfter.getInt64("score")).isEqualTo(initialScores[i]);

                Struct update = (Struct) keyRecords.get(1).value();
                assertThat(update.get("op")).isEqualTo("u");
                // Each key's update must reflect exactly its own prior and new state -
                // not another key's name or score that happened to be processed nearby.
                Struct updateBefore = update.getStruct("before");
                assertThat(updateBefore.getString("name")).isEqualTo(names[i]);
                assertThat(updateBefore.getInt64("score")).isEqualTo(initialScores[i]);

                Struct updateAfter = update.getStruct("after");
                assertThat(updateAfter.getString("name")).isEqualTo(names[i]);
                assertThat(updateAfter.getInt64("score")).isEqualTo(updatedScores[i]);
            }

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
