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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * Parameterized across both partition modes; each test creates and drops its own
 * partition-mode-suffixed tables/change stream per invocation.
 *
 * <p>This test is {@link RealSpannerCompatible}: when {@code -Dspanner.test.real=true} is
 * passed it runs against a real Cloud Spanner instance; otherwise it runs against the local
 * emulator.
 */
@RealSpannerCompatible
public class InterleavedTableIT extends AbstractSpannerConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(InterleavedTableIT.class);

    private static final String parentTableNamePrefix = "embedded_interleaved_parent_table";
    private static final String childTableNamePrefix = "embedded_interleaved_child_table";
    private static final String changeStreamNamePrefix = "embeddedInterleavedChangeStream";

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldCaptureCascadingDeleteOfInterleavedChildRows(PartitionMode partitionMode, Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String parentTable = tableFor(parentTableNamePrefix, partitionMode, dialect);
        String childTable = tableFor(childTableNamePrefix, partitionMode, dialect);
        String stream = streamFor(changeStreamNamePrefix, partitionMode, dialect);

        String tableParams = "(id INT64, name STRING(100)) PRIMARY KEY (id)";
        connection.createTable(parentTable, tableParams);

        tableParams = "(id INT64, child_id INT64, value STRING(100)) PRIMARY KEY (id, child_id), "
                + "INTERLEAVE IN PARENT " + parentTable + " ON DELETE CASCADE";
        connection.createTable(childTable, tableParams);
        connection.createChangeStream(stream, partitionMode, parentTable, childTable);
        try {
            final Configuration config = buildTestConfig(base, stream, parentTable, partitionMode);

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // One atomic transaction inserting the parent and its interleaved child.
            connection.executeUpdate(List.of(
                    "INSERT INTO " + parentTable + "(id, name) VALUES (1, 'Alice')",
                    "INSERT INTO " + childTable + "(id, child_id, value) VALUES (1, 100, 'Item1')"));

            // Only the parent is deleted explicitly - the child row is removed purely by
            // the ON DELETE CASCADE relationship, with no DML statement of its own.
            connection.executeUpdate(
                    "DELETE FROM " + parentTable + " WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(20, false);

            List<SourceRecord> parentRecords = sourceRecords.recordsForTopic(getTopicName(config, parentTable));
            List<SourceRecord> childRecords = sourceRecords.recordsForTopic(getTopicName(config, childTable));

            // MUTABLE_KEY_RANGE with multiple tasks can legitimately redeliver the boundary
            // delete before a window closes or a sequence boundary is persisted. Tolerate
            // duplicates while still verifying c -> d -> tombstone order and transaction IDs.
            assertThat(parentRecords).hasSizeGreaterThanOrEqualTo(3);
            assertThat(childRecords).hasSizeGreaterThanOrEqualTo(3);

            Struct parentInsert = firstOp(parentRecords, "c");
            Struct childInsert = firstOp(childRecords, "c");
            assertThat(transactionId(childInsert)).isEqualTo(transactionId(parentInsert));

            Struct parentDelete = firstOp(parentRecords, "d");
            assertThat(parentDelete.getStruct("before").getString("name")).isEqualTo("Alice");

            Struct childDelete = firstOp(childRecords, "d");
            assertThat(childDelete.getStruct("before").getString("value")).isEqualTo("Item1");
            assertThat(transactionId(childDelete)).isEqualTo(transactionId(parentDelete));

            assertThat(hasTombstoneAfter(parentRecords, firstOpIndex(parentRecords, "d"))).isTrue();
            assertThat(hasTombstoneAfter(childRecords, firstOpIndex(childRecords, "d"))).isTrue();

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            stopConnector();
            connection.dropChangeStream(stream);
            connection.dropTable(childTable);
            connection.dropTable(parentTable);
        }
    }

    private static Struct firstOp(List<SourceRecord> records, String op) {
        return records.stream()
                .filter(r -> r.value() != null)
                .map(r -> (Struct) r.value())
                .filter(s -> op.equals(s.getString("op")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No " + op + " record found"));
    }

    private static int firstOpIndex(List<SourceRecord> records, String op) {
        for (int i = 0; i < records.size(); i++) {
            SourceRecord r = records.get(i);
            if (r.value() != null && op.equals(((Struct) r.value()).getString("op"))) {
                return i;
            }
        }
        throw new AssertionError("No " + op + " record found");
    }

    private static boolean hasTombstoneAfter(List<SourceRecord> records, int index) {
        for (int i = index + 1; i < records.size(); i++) {
            if (records.get(i).value() == null) {
                return true;
            }
        }
        return false;
    }

    private static String transactionId(Struct value) {
        return value.getStruct("source").getString("server_transaction_id");
    }
}
