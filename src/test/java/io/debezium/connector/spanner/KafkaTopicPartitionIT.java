/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

public class KafkaTopicPartitionIT extends AbstractSpannerConnectorIT {

    private static final String tableName = "kafka_topic_partition_tests_table";
    private static final String changeStreamName = "kafkaTopicPartitionChangeStream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        databaseConnection.createTable(tableName + "(id int64, name string(100),time TIMESTAMP,\n" +
                "  date DATE,\n" +
                "  byt BYTES(2000),\n" +
                "  bool BOOL, long_time int64) primary key(id)");

        databaseConnection.createChangeStream(changeStreamName, tableName);

        System.out.println("KafkaTopicPartitionIT is ready...");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(tableName);
    }

    @Test
    public void checkRecordsWithSameKeyAreInSamePartition() throws InterruptedException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();
        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();
        databaseConnection.executeUpdate("insert into " + tableName + "(id, name) values (1, 'some name')");
        databaseConnection.executeUpdate("update " + tableName + " set name = 'test' where id = 1");
        databaseConnection.executeUpdate("insert into " + tableName + "(id, name) values (2, 'test name')");
        databaseConnection.executeUpdate("update " + tableName + " set bool = true where id = 2");
        waitForCDC();
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
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

}
