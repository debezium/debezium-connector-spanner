/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

@RealSpannerCompatible
public class LowWatermarkRecordIT extends AbstractSpannerConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(LowWatermarkRecordIT.class);

    private static final String tablePrefix = "low_watermark_record_tests_table";
    private static final String changeStreamPrefix = "lowWatermarkRecordTestChangeStream";

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void shouldStreamUpdatesToKafka(Dialect dialect) throws InterruptedException {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(tablePrefix, null, dialect);
        String stream = streamFor(changeStreamPrefix, null, dialect);

        createTableAndStream(connection, PartitionMode.IMMUTABLE_KEY_RANGE, table, stream);
        try {
            Instant now = Instant.now();
            final Configuration config = Configuration.copy(base)
                    .with("gcp.spanner.change.stream", stream)
                    .with("name", table + "_test")
                    .with("gcp.spanner.start.time",
                            DateTimeFormatter.ISO_INSTANT.format(now))
                    .with("gcp.spanner.low-watermark.enabled", true)
                    .build();

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            connection.executeUpdate("insert into " + table + "(id, value) values (1, 'some value')");
            connection.executeUpdate("update " + table + " set value = 'test' where id = 1");

            waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            List<Long> lowWatermarks = records.stream()
                    .map(rec -> rec.value() != null
                            ? (Long) ((Struct) ((Struct) rec.value()).get("source")).get("low_watermark")
                            : null)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            assertThat(!lowWatermarks.isEmpty());
            assertThat(Collections.max(lowWatermarks) > now.plus(2, ChronoUnit.SECONDS).toEpochMilli());
            validateLowWatermarks(records, lowWatermarks);

            stopConnector();
            assertConnectorNotRunning();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    private void validateLowWatermarks(List<SourceRecord> records, List<Long> lowWatermarks) {
        for (SourceRecord record : records) {
            if (record.timestamp() != null) {
                assertTrue(record.timestamp().longValue() > lowWatermarks.get(0).longValue());
            }
        }
    }
}
