/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.util.Testing;

public class DataTypesIT extends AbstractSpannerConnectorIT {

    private static final String gsqlTableName = "g_embedded_data_types_tests_table";
    private static final String gsqlChangeStreamName = "g_embeddedDataTypesTestChangeStream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {

        databaseConnection.createTable(gsqlTableName + "(id INT64,"
                + "  boolcol BOOL,"
                + "  int64col INT64,"
                + "  float32col FLOAT32,"
                + "  float64col FLOAT64,"
                + "  timestampcol TIMESTAMP,"
                + "  datecol DATE,"
                + "  stringcol STRING(MAX),"
                + "  bytescol BYTES(MAX),"
                + "  numericcol NUMERIC,"
                + "  jsoncol JSON,"
                + "  arrcol ARRAY<STRING(MAX)>,"
                + "  tokenlistcol TOKENLIST AS (TOKENIZE_FULLTEXT(stringcol)) HIDDEN, "
                + ") PRIMARY KEY (id)");
        databaseConnection.createChangeStream(gsqlChangeStreamName, gsqlTableName);

        Testing.print("DataTypesIT is ready...");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(gsqlChangeStreamName);
        databaseConnection.dropTable(gsqlTableName);
    }

    @Test
    public void shouldStreamUpdatesToKafkaWithTheCorrectType()
            throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", gsqlChangeStreamName)
                .with("name", gsqlTableName + "_test")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();
        final long insertedRows = databaseConnection.executeUpdate("INSERT INTO " + gsqlTableName
                + "(id"
                + ", boolcol"
                + ", int64col"
                + ", float32col, float64col"
                + ", timestampcol"
                + ", datecol"
                + ", stringcol"
                + ", bytescol"
                + ", numericcol"
                + ", jsoncol"
                + ", arrcol"
                + ") "
                + "VALUES (1"
                + ", true"
                + ", 42"
                + ", 3.14"
                + ", 2.71"
                + ", '1970-01-01 00:00:00 UTC',"
                + " '1970-01-01'"
                + ", 'stringVal'"
                + ", b'bytesVal'"
                + ", 6.023,"
                + " JSON '\"Hello\"'"
                + ", ['a', 'b'])");

        assertEquals(1, insertedRows);

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, gsqlTableName));
        assertThat(records).hasSize(1);

        Struct record = (Struct) (records.get(0).value());
        assertThat(record.get("op")).isEqualTo("c");
        assertThat(record.schema().field("after")).isNotNull();

        Struct values = record.getStruct("after");

        assertTrue(values.getBoolean("boolcol"));
        assertThat(values.getInt64("int64col")).isEqualTo(42);
        assertThat(values.getFloat32("float32col")).isEqualTo(3.14f);
        assertThat(values.getFloat64("float64col")).isEqualTo(2.71);
        assertThat(values.getString("timestampcol")).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(values.getString("datecol")).isEqualTo("1970-01-01");
        assertThat(values.getString("stringcol")).isEqualTo("stringVal");
        assertThat(values.getBytes("bytescol")).isEqualTo("bytesVal".getBytes());
        assertThat(values.getString("numericcol")).isEqualTo("6.023");
        assertThat(values.getString("jsoncol")).isEqualTo("\"Hello\"");
        assertThat(values.getArray("arrcol")).containsExactly("a", "b");
        assertThat(values.getString("tokenlistcol")).isNull();

        stopConnector();
        assertConnectorNotRunning();
    }
}
