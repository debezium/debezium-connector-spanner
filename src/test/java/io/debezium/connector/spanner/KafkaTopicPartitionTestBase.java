/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

public class KafkaTopicPartitionTestBase extends AbstractSpannerConnectorIT {

    private static final String tablePrefix = "kafka_topic_partition_tests_table";
    private static final String changeStreamPrefix = "kafkaTopicPartitionChangeStream";

    public void checkRecordsWithSameKeyAreInSamePartition(Dialect dialect, Logger logger) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect, logger);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(tablePrefix, null, dialect);
        String stream = streamFor(changeStreamPrefix, null, dialect);

        String tableParams = "(id int64, name string(100),time TIMESTAMP,\n" +
                "  date DATE,\n" +
                "  byt BYTES(2000),\n" +
                "  bool_val BOOL, long_time int64) primary key(id)";
        connection.createTable(table, tableParams);
        connection.createChangeStream(stream, PartitionMode.IMMUTABLE_KEY_RANGE, table);
        try {
            final Configuration config = buildTestConfig(base, stream, table, PartitionMode.IMMUTABLE_KEY_RANGE);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate("insert into " + table + "(id, name) values (1, 'some name')");
            connection.executeUpdate("update " + table + " set name = 'test' where id = 1");
            connection.executeUpdate("insert into " + table + "(id, name) values (2, 'test name')");
            connection.executeUpdate("update " + table + " set bool_val = true where id = 2");

            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            assertThat(records).hasSize(4); // 2 * (insert + update)
            Map<Object, List<SourceRecord>> keyToRecords = records.stream()
                    .collect(Collectors.groupingBy(SourceRecord::key));
            assertThat(keyToRecords).hasSize(2);
            keyToRecords.values().forEach(keyRecordsGroup -> {
                assertEquals(2, keyRecordsGroup.size());
                SourceRecord record1 = keyRecordsGroup.get(0);
                SourceRecord record2 = keyRecordsGroup.get(1);
                long commitTimestamp1 = (Long) ((Struct) (record1.value())).get("ts_ms");
                long commitTimestamp2 = (Long) ((Struct) (record2.value())).get("ts_ms");
                assertTrue(commitTimestamp1 <= commitTimestamp2);
                assertEquals(1, keyRecordsGroup.stream()
                        .map(SourceRecord::sourcePartition)
                        .collect(Collectors.toSet()).size());
            });

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

}
