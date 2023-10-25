/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

public class BasicSanityCheckIT extends AbstractSpannerConnectorIT {

    private static final String tableName = "embedded_sanity_tests_table";
    private static final String changeStreamName = "embeddedSanityTestChangeStream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        databaseConnection.createTable(tableName + "(id int64, name string(100)) primary key(id)");
        databaseConnection.createChangeStream(changeStreamName, tableName);

        System.out.println("BasicSanityCheckIT is ready...");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(tableName);
    }

    @Test
    public void shouldNotStartConnectorWithoutRequireConfigs() throws InterruptedException {
        // Config with only instance id provided.
        Configuration config = Configuration.create()
                .with("gcp.spanner.instance.id", database.getInstanceId())
                .build();
        start(SpannerConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(msg.contains("Connector configuration is not valid"));
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldNotStartConnectorWithoutNonExistentChangeStreams() throws InterruptedException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", "fooBar")
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();
        start(SpannerConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(msg.contains("ChangeStream 'fooBar' doesn't exist or you don't have sufficient permissions"));
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldNotStartConnectorWithOutOfRangeHeartbeatMillis() throws InterruptedException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("heartbeat.interval.ms", "1")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now().plus(2, ChronoUnit.DAYS)))
                .build();
        start(SpannerConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(msg.contains("Heartbeat interval must be between 100 and 300000"));
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldStreamUpdatesToKafka() throws InterruptedException {
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
        databaseConnection.executeUpdate("delete from " + tableName + " where id = 1");
        waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS);
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
        assertThat(records).hasSize(4);
        // Verify that mod types are create + update + delete + TOMBSTONE in order.
        assertThat((String) ((Struct) (records.get(0).value())).get("op")).isEqualTo("c");
        assertThat((String) ((Struct) (records.get(1).value())).get("op")).isEqualTo("u");
        assertThat((String) ((Struct) (records.get(2).value())).get("op")).isEqualTo("d");
        assertThat(records.get(3).value()).isEqualTo(null);
        stopConnector();
        assertConnectorNotRunning();
    }
}
