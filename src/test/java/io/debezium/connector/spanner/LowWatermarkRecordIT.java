/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

public class LowWatermarkRecordIT extends AbstractSpannerConnectorIT {

    private static final String tableName = "low_watermark_record_tests_table";
    private static final String changeStreamName = "lowWatermarkRecordTestChangeStream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        databaseConnection.createTable(tableName + "(id int64, name string(100)) primary key(id)");
        databaseConnection.createChangeStream(changeStreamName, tableName);

        System.out.println("LowWatermarkRecordIT is ready...");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(tableName);
    }

    @Test
    public void shouldStreamUpdatesToKafka() throws InterruptedException {
        Instant now = Instant.now();
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(now))
                .with("gcp.spanner.low-watermark.enabled", true)
                .build();
        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();
        databaseConnection.executeUpdate("insert into " + tableName + "(id, name) values (1, 'some name')");
        databaseConnection.executeUpdate("update " + tableName + " set name = 'test' where id = 1");
        waitForCDC();
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
        List<Long> lowWatermarks = records.stream()
                .map(rec -> rec.value() != null
                        ? (Long) ((Struct) ((Struct) rec.value()).get("source")).get("low_watermark")
                        : null)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        assertThat(!lowWatermarks.isEmpty());
        assertThat(Collections.max(lowWatermarks) > now.plus(2,
                ChronoUnit.SECONDS).toEpochMilli());
        validateLowWatermarks(records, lowWatermarks);
        stopConnector();
        assertConnectorNotRunning();
    }

    private void validateLowWatermarks(List<SourceRecord> records, List<Long> lowWatermarks) {
        for (SourceRecord record : records) {
            if (record.timestamp() != null) {
                assertTrue(record.timestamp().longValue() > lowWatermarks.get(0).longValue());
            }
        }
    }
}
