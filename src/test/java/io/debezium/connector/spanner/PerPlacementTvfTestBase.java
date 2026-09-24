/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assumptions;
import org.slf4j.Logger;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

public class PerPlacementTvfTestBase extends AbstractSpannerConnectorIT {

    static {
        // Per-placement TVF reads start three independent change-stream partitions on real
        // Cloud Spanner, and the unassigned/default placement can lag behind the named
        // placements. The default 30 s record wait time is often too tight; raise it for
        // real-Spanner runs without overriding an explicit user-supplied -D value.
        if (Connection.isRealSpanner()) {
            System.setProperty("debezium.test.records.waittime",
                    System.getProperty("debezium.test.records.waittime", "90"));
            System.setProperty("debezium.test.records.waittime.after.nulls",
                    System.getProperty("debezium.test.records.waittime.after.nulls", "5"));
        }
    }

    private static final String EAST_INSTANCE_PARTITION = System.getProperty(
            "spanner.test.east.instance.partition", "east-partition");
    private static final String WEST_INSTANCE_PARTITION = System.getProperty(
            "spanner.test.west.instance.partition", "west-partition");
    private static final String EAST_PLACEMENT = System.getProperty(
            "spanner.test.east.placement", "PlacementMoveEast");
    private static final String WEST_PLACEMENT = System.getProperty(
            "spanner.test.west.placement", "PlacementMoveWest");

    public void shouldReadEachPlacementOnlyFromItsTvf(Dialect dialect, Logger logger) throws Exception {
        Assumptions.assumeTrue(Connection.isRealSpanner(),
                "Per-placement TVF tests require real Cloud Spanner. Run with -Dspanner.test.real=true.");

        Connection connection = connectionFor(dialect, logger);
        Configuration base = baseConfigFor(dialect);
        connection.createPlacementIfMissing(EAST_PLACEMENT, EAST_INSTANCE_PARTITION);
        connection.createPlacementIfMissing(WEST_PLACEMENT, WEST_INSTANCE_PARTITION);

        String suffix = Long.toUnsignedString(System.nanoTime(), 36);
        String table = tableFor("placement_tvf_smoke_" + suffix, null, dialect);
        String stream = "pt" + suffix.substring(Math.max(0, suffix.length() - 8));

        connection.createTable(table,
                "(id INT64 NOT NULL, region STRING(MAX) NOT NULL PLACEMENT KEY, value STRING(MAX)) PRIMARY KEY (id)");
        boolean connectorStarted = false;
        try {
            connection.createPerPlacementTvfChangeStream(stream, table);
            List<String> tvfNames = connection.readPlacementTvfNames(stream);
            assertThat(tvfNames).hasSize(3);

            String eastTvf = tvfForPlacement(tvfNames, EAST_PLACEMENT);
            String westTvf = tvfForPlacement(tvfNames, WEST_PLACEMENT);
            String defaultTvf = tvfForPlacement(tvfNames, "default");
            Configuration config = Configuration.copy(
                    buildTestConfig(base, stream, table, PartitionMode.MUTABLE_KEY_RANGE))
                    .with("gcp.spanner.placement.tvf.names", String.join(",", tvfNames))
                    .with("tasks.max", 1)
                    .build();

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            connectorStarted = true;
            assertConnectorIsRunning();

            connection.executeUpdate("INSERT INTO " + table
                    + "(id, region, value) VALUES (1, '" + EAST_PLACEMENT + "', 'east-value')");
            connection.executeUpdate("INSERT INTO " + table
                    + "(id, region, value) VALUES (2, '" + WEST_PLACEMENT + "', 'west-value')");
            connection.executeUpdate("INSERT INTO " + table
                    + "(id, region, value) VALUES (3, 'default', 'default-value')");

            List<SourceRecord> records = consumeRecordsForTopic(config, table, 3);

            assertThat(records).hasSize(3);
            assertRecord(records, 1L, EAST_PLACEMENT, "east-value", eastTvf);
            assertRecord(records, 2L, WEST_PLACEMENT, "west-value", westTvf);
            assertRecord(records, 3L, "default", "default-value", defaultTvf);

            stopConnector();
            connectorStarted = false;
            assertConnectorNotRunning();
        }
        finally {
            if (connectorStarted) {
                stopConnector();
            }
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    private List<SourceRecord> consumeRecordsForTopic(Configuration config, String table, int expectedCount) throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(waitTimeForRecords());
        long pollStepSeconds = Math.min(5, waitTimeForRecords() / 3);
        do {
            waitForAvailableRecords(pollStepSeconds, TimeUnit.SECONDS);
            List<SourceRecord> polled = consumeRecordsByTopic(expectedCount - records.size(), false)
                    .recordsForTopic(getTopicName(config, table));
            if (polled != null) {
                records.addAll(polled);
            }
        } while (records.size() < expectedCount && System.nanoTime() < deadline);
        return records;
    }

    private static void assertRecord(List<SourceRecord> records, long id, String region, String value, String tvfName) {
        List<SourceRecord> matchingRecords = records.stream()
                .filter(record -> ((Struct) record.value()).getStruct("after").getInt64("id") == id)
                .toList();
        assertThat(matchingRecords).hasSize(1);
        SourceRecord record = matchingRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct("after");
        assertThat(after.getString("region")).isEqualTo(region);
        assertThat(after.getString("value")).isEqualTo(value);
        assertThat(SpannerPartition.extractTvfName(record.sourcePartition())).isEqualTo(tvfName);
    }

    private static String tvfForPlacement(List<String> tvfNames, String placement) {
        return tvfNames.stream()
                .filter(name -> name.replace("\"", "").toLowerCase(Locale.ROOT)
                        .endsWith("_" + placement.toLowerCase(Locale.ROOT)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No generated TVF for placement " + placement + ": " + tvfNames));
    }
}
