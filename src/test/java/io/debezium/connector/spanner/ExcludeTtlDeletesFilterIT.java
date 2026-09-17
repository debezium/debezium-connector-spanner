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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * The {@code exclude_ttl_deletes} change stream option filters out deletes caused by TTL
 * garbage collection while still delivering normal user-issued deletes.
 *
 * <p>Parameterized across both partition modes; each test creates and drops its own
 * partition-mode-suffixed table/change stream per invocation.
 *
 * <p>This test is {@link RealSpannerCompatible}: when {@code -Dspanner.test.real=true} is
 * passed it runs against a real Cloud Spanner instance; otherwise it runs against the local
 * emulator.
 */
@RealSpannerCompatible
public class ExcludeTtlDeletesFilterIT extends AbstractSpannerConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExcludeTtlDeletesFilterIT.class);

    private static final String tableNamePrefix = "exclude_ttl_deletes_filter_table";
    private static final String changeStreamNamePrefix = "excludeTtlDeletesFilterStream";

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldFilterOutTtlDeletesButStillDeliverUserIssuedDeletes(PartitionMode partitionMode, Dialect dialect) throws Exception {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(tableNamePrefix, partitionMode, dialect);
        String stream = streamFor(changeStreamNamePrefix, partitionMode, dialect);

        String tableParams = "(id INT64, value STRING(100), expire_at TIMESTAMP NOT NULL) PRIMARY KEY (id), "
                + "ROW DELETION POLICY (OLDER_THAN(expire_at, INTERVAL 1 DAY))";
        connection.createTable(table, tableParams);
        connection.createChangeStreamExcludeTtlDeletes(stream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            clearKafkaTopics();
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            boolean isPostgres = dialect == Dialect.POSTGRESQL;

            // Row 1 is already past its TTL expiry - its eventual GC-driven delete must be
            // filtered out entirely by exclude_ttl_deletes.
            String timestampSub = isPostgres ? "CURRENT_TIMESTAMP - INTERVAL '2 days'"
                    : "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)";
            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, value, expire_at) VALUES ("
                            + "1, 'ttl-expires-soon', " + timestampSub + ")");

            // Row 2 has a far-future expiry, so it will only ever be removed by the explicit
            // user-issued DELETE below - the filter must not affect it.
            String timestampAdd = isPostgres ? "CURRENT_TIMESTAMP + INTERVAL '30 days'"
                    : "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)";
            connection.executeUpdate(
                    "INSERT INTO " + table + "(id, value, expire_at) VALUES ("
                            + "2, 'not-expiring', " + timestampAdd + ")");
            connection.executeUpdate(
                    "DELETE FROM " + table + " WHERE id = 2");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));

            // 2 inserts + 1 user-issued delete + 1 tombstone - no TTL-triggered delete or
            // tombstone for row 1, no matter how long we wait for it.
            assertThat(records).hasSize(4);

            Struct userDelete = (Struct) records.get(2).value();
            assertThat(userDelete.get("op")).isEqualTo("d");
            assertThat(userDelete.getStruct("before").getInt64("id")).isEqualTo(2L);
            assertThat(userDelete.getStruct("source").getBoolean("system_transaction")).isFalse();

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
