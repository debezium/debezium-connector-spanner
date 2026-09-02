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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * The real-Cloud-Spanner placement-move scenarios, sharing a single {@code east}/{@code west}
 * placement pair provisioned once for the whole class, since {@code DROP PLACEMENT} alone
 * can take minutes to hours on the shared test instance. Each test still creates and drops
 * its own tables/change stream inline - only the placements are shared.
 *
 * <p>Needs geo-partitioning, which only a real Cloud Spanner instance supports. Expects the
 * pre-provisioned {@code east-partition}/{@code west-partition} instance partitions (see
 * {@code doc/real-spanner-testing.md}).
 */
@RealSpannerCompatible
@Disabled("DROP PLACEMENT alone can take minutes to hours on the shared real-Spanner test "
        + "instance, making iteration on this suite expensive. Also requires the "
        + "east-partition/west-partition instance partitions to be pre-provisioned "
        + "(see doc/real-spanner-testing.md).")
public class PlacementMoveIT extends AbstractSpannerConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlacementMoveIT.class);

    private static final String placementEast = "PlacementMoveEast";
    private static final String placementWest = "PlacementMoveWest";

    @BeforeAll
    static void setup() throws Exception {
        Assumptions.assumeTrue(Connection.isRealSpanner(),
                "Skipping: PlacementMoveIT needs geo-partitioning, which only a real Cloud Spanner "
                        + "instance supports. Run with -Dspanner.test.real=true.");
        databaseConnection.createPlacement(placementEast, "east-partition");
        databaseConnection.createPlacement(placementWest, "west-partition");
        pgDatabaseConnection.createPlacement(placementEast, "east-partition");
        pgDatabaseConnection.createPlacement(placementWest, "west-partition");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropPlacement(placementEast);
        databaseConnection.dropPlacement(placementWest);
        pgDatabaseConnection.dropPlacement(placementEast);
        pgDatabaseConnection.dropPlacement(placementWest);
    }

    private static final String tablePrefix = "placement_key_move_table";
    private static final String changeStreamPrefix = "placementKeyMoveStream";

    /**
     * Core placement-table scenario: a row physically moves between placements, and a
     * follow-up write to the same row must not be delivered out of order relative to that
     * move. This is the property that actually depends on correct
     * {@code PartitionEventRecord} move-in/move-out handling.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void shouldOrderRecordsCorrectlyWhenRowMovesBetweenPlacements(Dialect dialect) throws Exception {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(tablePrefix, null, dialect);
        String stream = streamFor(changeStreamPrefix, null, dialect);

        String tableParams = "(id INT64 NOT NULL, region STRING(MAX) NOT NULL PLACEMENT KEY, value STRING(MAX)) PRIMARY KEY (id)";
        connection.createTable(table, tableParams);
        connection.createMutableKeyRangeChangeStream(stream, table);
        try {
            final Configuration config = buildTestConfig(base, stream, table, PartitionMode.MUTABLE_KEY_RANGE);

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            // Row starts in the "east" placement.
            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, region, value) VALUES (1, '" + placementEast + "', 'v1')");

            // Changing the placement key physically moves the row's data to the other
            // instance partition. This is the operation that triggers PartitionEventRecord
            // move-out (on the source partition) and move-in (on the destination partition).
            connection.executeUpdate(
                    "UPDATE " + table + " SET region = '" + placementWest + "' WHERE id = 1");

            // A follow-up write to the SAME row, issued immediately after the move, so we
            // can check it isn't processed out of order relative to the move itself.
            connection.executeUpdate(
                    "UPDATE " + table + " SET value = 'v2' WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(3); // insert + move + follow-up

            Struct moveRecord = (Struct) records.get(1).value();
            assertThat(moveRecord.get("op")).isEqualTo("u");
            assertThat(moveRecord.getStruct("before").getString("region")).isEqualTo(placementEast);
            assertThat(moveRecord.getStruct("after").getString("region")).isEqualTo(placementWest);

            // The follow-up write to the new placement must be strictly ordered after the
            // move, with a later commit timestamp - if the connector read the destination
            // partition before the source partition had caught up to the move's
            // commit_timestamp, this write could be delivered out of order or duplicated.
            Struct followUpRecord = (Struct) records.get(2).value();
            assertThat(followUpRecord.getStruct("after").getString("value")).isEqualTo("v2");
            long moveTimestamp = moveRecord.getStruct("source").getInt64("ts_ms");
            long followUpTimestamp = followUpRecord.getStruct("source").getInt64("ts_ms");
            assertThat(followUpTimestamp).isGreaterThan(moveTimestamp);

            // The connector doesn't expose move events as Kafka records or a SourceInfo field,
            // so only this downstream ordering effect is verifiable here, not move-event content.

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    private static final String interleavedParentTablePrefix = "placement_parent_move_table";
    private static final String interleavedChildTablePrefix = "placement_child_move_table";
    private static final String interleavedChangeStreamPrefix = "placementParentChildMoveStream";

    /**
     * Interleaved-in-placement scenario: a child table has no placement key of its own - it
     * is always physically co-located with its parent row, so it must move with the parent
     * whenever the parent's placement key changes.
     *
     * <p>The connector doesn't surface a move as a Kafka record or {@code SourceInfo} field for
     * either the parent or the child - {@code PartitionEventEvent} dispatch only drives internal
     * move-out/processed-timestamp bookkeeping via {@code PartitionManager}. So correct ordering
     * of a follow-up child write relative to the parent's move is the only observable signal that
     * the child moved with it.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void shouldMoveInterleavedChildRowsWithParentPlacementChange(Dialect dialect) throws Exception {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String parentTable = tableFor(interleavedParentTablePrefix, null, dialect);
        String childTable = tableFor(interleavedChildTablePrefix, null, dialect);
        String stream = streamFor(interleavedChangeStreamPrefix, null, dialect);

        String tableParams = "(id INT64 NOT NULL, region STRING(MAX) NOT NULL PLACEMENT KEY, name STRING(MAX)) PRIMARY KEY (id)";
        connection.createTable(parentTable, tableParams);

        // Interleaved children of a placement table have no placement key of their own -
        // they are physically co-located with the parent row and move with it.
        tableParams = "(id INT64 NOT NULL, child_id INT64 NOT NULL, value STRING(MAX)) "
                + "PRIMARY KEY (id, child_id), INTERLEAVE IN PARENT " + parentTable + " ON DELETE CASCADE";
        connection.createTable(childTable, tableParams);
        connection.createMutableKeyRangeChangeStream(stream, parentTable, childTable);
        try {
            final Configuration config = buildTestConfig(base, stream, parentTable, PartitionMode.MUTABLE_KEY_RANGE);

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + parentTable + "(id, region, name) VALUES (1, '" + placementEast + "', 'Alice')");
            connection.executeUpdate(
                    "INSERT INTO " + childTable + "(id, child_id, value) VALUES (1, 100, 'Item1')");

            // Moving the parent's placement must move the child's physical storage too,
            // since interleaved rows are always co-located with their parent.
            connection.executeUpdate(
                    "UPDATE " + parentTable + " SET region = '" + placementWest + "' WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(20, false);

            List<SourceRecord> parentRecords = sourceRecords.recordsForTopic(getTopicName(config, parentTable));
            Struct parentMoveRecord = (Struct) parentRecords.get(1).value(); // after the insert
            assertThat(parentMoveRecord.getStruct("after").getString("region")).isEqualTo(placementWest);

            connection.executeUpdate(
                    "UPDATE " + childTable + " SET value = 'Item1-updated' WHERE id = 1 AND child_id = 100");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords childUpdateRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> childRecords = childUpdateRecords.recordsForTopic(getTopicName(config, childTable));
            Struct childUpdateRecord = (Struct) childRecords.get(childRecords.size() - 1).value();
            long parentMoveTimestamp = parentMoveRecord.getStruct("source").getInt64("ts_ms");
            long childUpdateTimestamp = childUpdateRecord.getStruct("source").getInt64("ts_ms");
            assertThat(childUpdateTimestamp).isGreaterThan(parentMoveTimestamp);

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(childTable);
            connection.dropTable(parentTable);
        }
    }

    private static final String cascadeParentTablePrefix = "placement_parent_cascade_table";
    private static final String cascadeChildTablePrefix = "placement_child_cascade_table";
    private static final String cascadeChangeStreamPrefix = "placementCascadeDuringMoveStream";

    /**
     * The most complex of the three placement scenarios: combines {@link InterleavedTableIT}'s
     * proven cascade-delete mechanism with an in-flight placement move, to check whether the
     * connector's partition-move bookkeeping and its cascade-delete handling agree on which
     * partition owns the key at the moment of deletion.
     */
    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void shouldOrderCascadingDeleteCorrectlyRelativeToInFlightPlacementMove(Dialect dialect) throws Exception {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String parentTable = tableFor(cascadeParentTablePrefix, null, dialect);
        String childTable = tableFor(cascadeChildTablePrefix, null, dialect);
        String stream = streamFor(cascadeChangeStreamPrefix, null, dialect);

        String tableParams = "(id INT64 NOT NULL, region STRING(MAX) NOT NULL PLACEMENT KEY, name STRING(MAX)) "
                + "PRIMARY KEY (id)";
        connection.createTable(parentTable, tableParams);

        tableParams = "(id INT64 NOT NULL, child_id INT64 NOT NULL, value STRING(MAX)) "
                + "PRIMARY KEY (id, child_id), INTERLEAVE IN PARENT " + parentTable + " ON DELETE CASCADE";
        connection.createTable(childTable, tableParams);
        connection.createMutableKeyRangeChangeStream(stream, parentTable, childTable);
        try {
            final Configuration config = buildTestConfig(base, stream, parentTable, PartitionMode.MUTABLE_KEY_RANGE);

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate(
                    "INSERT INTO " + parentTable + "(id, region, name) VALUES (1, '" + placementEast + "', 'Alice')");
            connection.executeUpdate(
                    "INSERT INTO " + childTable + "(id, child_id, value) VALUES (1, 100, 'Item1')");

            connection.executeUpdate(
                    "UPDATE " + parentTable + " SET region = '" + placementWest + "' WHERE id = 1");

            // Deleting the parent immediately after the move cascades to the child, exactly
            // as InterleavedTableIT proved for a non-placement parent - but now the delete
            // has to be correctly attributed to whichever partition the row had just moved
            // to. If the connector's partition-move bookkeeping and its cascade-delete
            // handling don't agree on which partition currently owns this key, this is
            // exactly the kind of place a duplicate or dropped delete could hide.
            connection.executeUpdate(
                    "DELETE FROM " + parentTable + " WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(20, false);

            List<SourceRecord> parentRecords = sourceRecords.recordsForTopic(getTopicName(config, parentTable));
            List<SourceRecord> childRecords = sourceRecords.recordsForTopic(getTopicName(config, childTable));
            // insert + move + delete + tombstone, for both parent and child.
            assertThat(parentRecords).hasSize(4);
            assertThat(childRecords).hasSize(3); // insert + cascaded delete + tombstone (no move-triggered record on the child)

            Struct parentDelete = (Struct) parentRecords.get(2).value();
            Struct childDelete = (Struct) childRecords.get(1).value();
            assertThat(parentDelete.get("op")).isEqualTo("d");
            assertThat(childDelete.get("op")).isEqualTo("d");

            // Both deletes must share one transaction identity, even
            // though the parent had just moved placements.
            assertThat(childDelete.getStruct("source").getString("server_transaction_id"))
                    .isEqualTo(parentDelete.getStruct("source").getString("server_transaction_id"));

            // Exactly one delete + one tombstone per key - no duplicate delivery caused by
            // the move and the cascade both trying to "explain" the same row disappearing.
            assertThat(parentRecords.get(3).value()).isNull(); // tombstone
            assertThat(childRecords.get(2).value()).isNull(); // tombstone

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(childTable);
            connection.dropTable(parentTable);
        }
    }
}
