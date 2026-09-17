/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

public class BasicSanityCheckTestBase extends AbstractSpannerConnectorIT {

    private static final String tablePrefix = "embedded_sanity_tests_table";
    private static final String changeStreamPrefix = "embeddedSanityTestChangeStream";

    public void shouldStreamUpdatesToKafka(Dialect dialect, Logger logger) throws InterruptedException {
        Connection connection = connectionFor(dialect, logger);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(tablePrefix, null, dialect);
        String stream = streamFor(changeStreamPrefix, null, dialect);

        createTableAndStream(connection, PartitionMode.IMMUTABLE_KEY_RANGE, table, stream);
        try {
            final Configuration config = buildTestConfig(base, stream, table, PartitionMode.IMMUTABLE_KEY_RANGE);
            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate("insert into " + table + "(id, value) values (1, 'some value')");
            connection.executeUpdate("update " + table + " set value = 'test' where id = 1");
            connection.executeUpdate("delete from " + table + " where id = 1");

            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(4);

            // Verify that mod types are create + update + delete + TOMBSTONE in order.
            assertThat((String) ((Struct) (records.get(0).value())).get("op")).isEqualTo("c");
            assertThat((String) ((Struct) (records.get(1).value())).get("op")).isEqualTo("u");
            assertThat((String) ((Struct) (records.get(2).value())).get("op")).isEqualTo("d");
            assertThat(records.get(3).value()).isEqualTo(null);

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }
}
