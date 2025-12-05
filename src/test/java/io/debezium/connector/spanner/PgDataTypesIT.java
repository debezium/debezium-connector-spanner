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

public class PgDataTypesIT extends AbstractSpannerConnectorIT {

    private static final String pgsqlTableName = "pgembedded_data_types_tests_table";
    private static final String pgsqlChangeStreamName = "pgembeddeddatatypestestchangestream";

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        pgDatabaseConnection.createTable(pgsqlTableName + "(id bigint,"
                + "  boolcol boolean,"
                + "  int64col bigint,"
                + "  float64col float8,"
                + "  timestampcol timestamptz,"
                + " datecol date,"
                + " stringcol varchar,"
                + " bytescol bytea,"
                + " arrcol varchar[],"
                // TODO: enable float4 after emulator supports it.
                // + " float32col float4,"
                // TODO: debug why numeric and jsonb tests fail in PG dialect.
                // + " numericcol numeric,"
                // + " jsoncol jsonb,"
                + " PRIMARY KEY (id)"
                + ")");
        pgDatabaseConnection.createChangeStream(pgsqlChangeStreamName, pgsqlTableName);

        Testing.print("PgDataTypesIT is ready...");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        pgDatabaseConnection.dropChangeStream(pgsqlChangeStreamName);
        pgDatabaseConnection.dropTable(pgsqlTableName);
    }

    @Test
    public void shouldStreamUpdatesToKafkaWithTheCorrectType()
            throws InterruptedException, ExecutionException {
        final Configuration config = Configuration.copy(basePgConfig)
                .with("gcp.spanner.change.stream", pgsqlChangeStreamName)
                .with("name", pgsqlTableName + "_test")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();
        final long insertedRows = pgDatabaseConnection.executeUpdate("INSERT INTO " + pgsqlTableName
                + "(id"
                + ", boolcol"
                + ", int64col"
                + ", float64col"
                + ", timestampcol"
                + ", datecol"
                + ", stringcol"
                + ", bytescol"
                + ", arrcol"
                // + ", float32col"
                // + ", numericcol"
                // + ", jsoncol"
                + ") "
                + "VALUES (1"
                + ", true"
                + ", 42"
                + ", 2.71"
                + ", '1970-01-01 00:00:00 UTC'"
                + ", '1970-01-01'"
                + ", 'stringVal'"
                + ", bytea 'bytesVal'"
                // + ", 3.14"
                // + ", 6.023"
                // + ", JSONB '\"Hello\"'"
                + ", ARRAY['a', 'b']"
                + ")");

        assertEquals(1, insertedRows);

        assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, pgsqlTableName));
        assertThat(records).hasSize(1);

        Struct record = (Struct) (records.get(0).value());
        assertThat(record.get("op")).isEqualTo("c");
        assertThat(record.schema().field("after")).isNotNull();

        Struct values = record.getStruct("after");

        assertTrue(values.getBoolean("boolcol"));
        assertThat(values.getInt64("int64col")).isEqualTo(42);
        assertThat(values.getFloat64("float64col")).isEqualTo(2.71);
        assertThat(values.getString("timestampcol")).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(values.getString("datecol")).isEqualTo("1970-01-01");
        assertThat(values.getString("stringcol")).isEqualTo("stringVal");
        assertThat(values.getBytes("bytescol")).isEqualTo("bytesVal".getBytes());
        // TODO: enable float4 after emulator supports it.
        // assertThat(values.getFloat32("float32col")).isEqualTo(3.14f);
        // TODO: debug why numeric and jsonb tests fail in PG dialect.
        // assertThat(values.getString("numericcol")).isEqualTo("6.023");
        // assertThat(values.getString("jsoncol")).isEqualTo("\"Hello\"");
        assertThat(values.getArray("arrcol")).containsExactly("a", "b");

        stopConnector();
        assertConnectorNotRunning();
    }
}
