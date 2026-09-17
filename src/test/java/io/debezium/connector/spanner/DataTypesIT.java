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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.PartitionMode;

/**
 * Parameterized across both partition modes; each test creates and drops its own
 * partition-mode-suffixed table/change stream per invocation.
 *
 * <p>This test is {@link RealSpannerCompatible}: when {@code -Dspanner.test.real=true} is
 * passed it runs against a real Cloud Spanner instance; otherwise it runs against the local
 * emulator.
 */
@RealSpannerCompatible
public class DataTypesIT extends AbstractSpannerConnectorIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataTypesIT.class);

    private static final String tableNamePrefix = "embedded_data_types_tests_table";
    private static final String changeStreamNamePrefix = "embeddedDataTypesTestChangeStream";

    private static final String edgeCasesTableNamePrefix = "embedded_data_type_edge_cases_table";
    private static final String edgeCasesChangeStreamNamePrefix = "embeddedDataTypeEdgeCasesStream";

    @BeforeEach
    void clearTopics() {
        clearKafkaTopics();
    }

    @AfterEach
    void ensureConnectorStopped() throws InterruptedException {
        stopConnector();
        assertConnectorNotRunning();
    }

    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldStreamUpdatesToKafkaWithTheCorrectType(PartitionMode partitionMode, Dialect dialect)
            throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(tableNamePrefix, partitionMode, dialect);
        String stream = streamFor(changeStreamNamePrefix, partitionMode, dialect);

        String tableParams = "(id INT64,"
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
                + ") PRIMARY KEY (id)";
        connection.createTable(table, tableParams);
        connection.createChangeStream(stream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            boolean isPostgres = dialect == Dialect.POSTGRESQL;

            String bytesValue = isPostgres
                    ? "'bytesVal'"
                    : "b'bytesVal'";
            String jsonValue = isPostgres
                    ? "'\"Hello\"'"
                    : "JSON '\"Hello\"'";
            String arrayValue = isPostgres
                    ? "ARRAY['a', 'b']"
                    : "['a', 'b']";

            final long insertedRows = connection.executeUpdate("INSERT INTO " + table
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
                    + ", '1970-01-01 00:00:00 UTC'"
                    + ", '1970-01-01'"
                    + ", 'stringVal'"
                    + ", " + bytesValue
                    + ", 6.023"
                    + ", " + jsonValue
                    + ", " + arrayValue
                    + ")");

            assertEquals(1, insertedRows);

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
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
            if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
                // TOKENLIST is only relevant for GOOGLE_STANDARD_SQL dialect.
                assertThat(values.getString("tokenlistcol")).isNull();
            }
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }

    /**
     * Covers additional data-type edge cases
     * empty string vs. {@code NULL}, empty {@code BYTES}, large/negative
     * {@code NUMERIC}, unicode content, and empty arrays - all on the insert path, plus an
     * update and delete to check these values survive beyond just being captured on
     * creation.
     */
    @ParameterizedTest
    @MethodSource("partitionModesAndDialects")
    public void shouldRoundTripEdgeCaseValuesAcrossInsertUpdateDelete(PartitionMode partitionMode, Dialect dialect) throws InterruptedException, ExecutionException {
        Connection connection = connectionFor(dialect, LOGGER);
        Configuration base = baseConfigFor(dialect);
        String table = tableFor(edgeCasesTableNamePrefix, partitionMode, dialect);
        String stream = streamFor(edgeCasesChangeStreamNamePrefix, partitionMode, dialect);

        String tableParams = "(id INT64, description STRING(100), tag_bytes BYTES(100), balance NUMERIC, "
                + "unicode_name STRING(100), tags ARRAY<STRING(50)>) PRIMARY KEY (id)";
        connection.createTable(table, tableParams);
        connection.createChangeStream(stream, partitionMode, table);
        try {
            final Configuration config = buildTestConfig(base, stream, table, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            boolean isPostgres = dialect == Dialect.POSTGRESQL;

            String emptyBytes = isPostgres ? "''"
                    : "b''";
            String emptyArray = isPostgres ? "'{}'"
                    : "[]";
            connection.executeUpdate(
                    "INSERT INTO " + table
                            + "(id, description, tag_bytes, balance, unicode_name, tags) VALUES ("
                            + "1, "
                            + "'', "
                            + emptyBytes + ", "
                            + "-123.456789, "
                            + "'日本語 café ☕', "
                            + emptyArray + ")");

            String tagBytes = isPostgres ? "'payload'"
                    : "b'payload'";
            String tags = isPostgres ? "ARRAY['a', 'b']"
                    : "['a', 'b']";
            connection.executeUpdate(
                    "UPDATE " + table + " SET description = NULL, tag_bytes = " + tagBytes + ", "
                            + "balance = 99999999999999999999.999999999, "
                            + "unicode_name = '北京 🎉', tags = " + tags + " WHERE id = 1");

            connection.executeUpdate(
                    "DELETE FROM " + table + " WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, table));
            // insert + update + delete + tombstone
            assertThat(records).hasSize(4);

            Struct insertAfter = ((Struct) records.get(0).value()).getStruct("after");
            // Empty string and empty bytes must round-trip as empty, not NULL.
            assertThat(insertAfter.getString("description")).isEqualTo("");
            assertThat(insertAfter.getBytes("tag_bytes")).isEqualTo(new byte[0]);
            assertThat(insertAfter.getString("balance")).isEqualTo("-123.456789");
            assertThat(insertAfter.getString("unicode_name")).isEqualTo("日本語 café ☕");
            assertThat(insertAfter.getArray("tags")).isEmpty();

            Struct updateRecord = (Struct) records.get(1).value();
            Struct updateBefore = updateRecord.getStruct("before");
            Struct updateAfter = updateRecord.getStruct("after");

            // Before reflects the original edge-case values.
            assertThat(updateBefore.getString("description")).isEqualTo("");
            assertThat(updateBefore.getArray("tags")).isEmpty();

            // After reflects the new values, including the empty-string-to-NULL transition
            // and a large positive NUMERIC replacing a negative one.
            assertThat(updateAfter.getString("description")).isNull();
            assertThat(updateAfter.getBytes("tag_bytes")).isEqualTo("payload".getBytes());
            assertThat(updateAfter.getString("balance")).isEqualTo("99999999999999999999.999999999");
            assertThat(updateAfter.getString("unicode_name")).isEqualTo("北京 🎉");
            assertThat(updateAfter.getArray("tags")).containsExactly("a", "b");

            Struct deleteBefore = ((Struct) records.get(2).value()).getStruct("before");
            // The delete's "before" must reflect the updated state, matching the same
            // pattern established in DeleteEventIT - not the original insert-time values.
            assertThat(deleteBefore.getString("unicode_name")).isEqualTo("北京 🎉");
            assertThat(deleteBefore.getArray("tags")).containsExactly("a", "b");

            // Tombstone.
            assertThat(records.get(3).value()).isNull();
        }
        finally {
            connection.dropChangeStream(stream);
            connection.dropTable(table);
        }
    }
}
