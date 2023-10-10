/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

public class GracefulRestartIT extends AbstractSpannerConnectorIT {

    private static final String tableName = "graceful_restart_tests_table";
    private static final String changeStreamName = "gracefulRestartChangeStream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        databaseConnection.createTable(tableName + "(id int64, name string(100)) primary key(id)");
        databaseConnection.createChangeStream(changeStreamName, tableName);

        System.out.println("GracefulRestartIT is ready...");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(tableName);
    }

    @Test
    public void checkUpdatesStreamedToKafka() throws InterruptedException {
        stopConnector();
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
        SourceRecords sourceRecords = consumeRecordsByTopic(5, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, tableName));
        assertThat(records).hasSize(1); // create
        stopConnector();
        assertConnectorNotRunning();
        databaseConnection.executeUpdate("update " + tableName + " set name = 'test' where id = 1");
        start(SpannerConnector.class, config);
        SourceRecords sourceRecords2 = consumeRecordsByTopic(10, false);
        List<SourceRecord> records2 = sourceRecords2.recordsForTopic(getTopicName(config, tableName));
        assertThat(records2).hasSizeGreaterThanOrEqualTo(1); // create + update
        assertThat((String) ((Struct) (records2.get(0).value())).get("op")).isEqualTo("c");
        stopConnector();
        assertConnectorNotRunning();
    }
}
