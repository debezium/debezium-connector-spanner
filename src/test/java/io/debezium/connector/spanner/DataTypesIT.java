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
import org.junit.jupiter.params.provider.EnumSource;

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

    /**
     * Override the inherited emulator connection/config with a real-Spanner pair when
     * {@code -Dspanner.test.real=true} is supplied; otherwise keep the parent's emulator pair.
     */
    protected static final Connection databaseConnection = Connection.isRealSpanner()
            ? RealSpannerTestSupport.getConnection(database)
            : AbstractSpannerConnectorIT.databaseConnection;
    protected static final Configuration baseConfig = Connection.isRealSpanner()
            ? createBaseConfigBuilder(database, true).build()
            : AbstractSpannerConnectorIT.baseConfig;

    private static final String gsqlTableNamePrefix = "g_embedded_data_types_tests_table";
    private static final String gsqlChangeStreamNamePrefix = "g_embeddedDataTypesTestChangeStream";

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
    @EnumSource(PartitionMode.class)
    public void shouldStreamUpdatesToKafkaWithTheCorrectType(PartitionMode partitionMode)
            throws InterruptedException, ExecutionException {
        String gsqlTableName = gsqlTableNamePrefix + "_" + partitionMode.name().toLowerCase();
        String gsqlChangeStreamName = gsqlChangeStreamNamePrefix + partitionMode.name();
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
        databaseConnection.createChangeStream(gsqlChangeStreamName, partitionMode, gsqlTableName);
        try {
            final Configuration config = buildTestConfig(baseConfig, gsqlChangeStreamName, gsqlTableName, partitionMode);

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
        }
        finally {
            databaseConnection.dropChangeStream(gsqlChangeStreamName);
            databaseConnection.dropTable(gsqlTableName);
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
    @EnumSource(PartitionMode.class)
    public void shouldRoundTripEdgeCaseValuesAcrossInsertUpdateDelete(PartitionMode partitionMode) throws InterruptedException, ExecutionException {
        String edgeCasesTableName = edgeCasesTableNamePrefix + "_" + partitionMode.name().toLowerCase();
        String edgeCasesChangeStreamName = edgeCasesChangeStreamNamePrefix + partitionMode.name();
        databaseConnection.createTable(edgeCasesTableName
                + "(id INT64, description STRING(100), tag_bytes BYTES(100), balance NUMERIC, "
                + "unicode_name STRING(100), tags ARRAY<STRING(50)>) PRIMARY KEY (id)");
        databaseConnection.createChangeStream(edgeCasesChangeStreamName, partitionMode, edgeCasesTableName);
        try {
            final Configuration config = buildTestConfig(baseConfig, edgeCasesChangeStreamName, edgeCasesTableName, partitionMode);

            initializeConnectorTestFramework();
            start(SpannerConnector.class, config);
            assertConnectorIsRunning();

            databaseConnection.executeUpdate(
                    "INSERT INTO " + edgeCasesTableName + "(id, description, tag_bytes, balance, unicode_name, tags) VALUES ("
                            + "1, "
                            + "'', " // empty string, not NULL
                            + "b'', " // empty bytes, not NULL
                            + "-123.456789, " // negative numeric
                            + "'日本語 café ☕', " // unicode content
                            + "[])"); // empty array, not NULL

            databaseConnection.executeUpdate(
                    "UPDATE " + edgeCasesTableName + " SET description = NULL, tag_bytes = b'payload', "
                            + "balance = 99999999999999999999.999999999, "
                            + "unicode_name = '北京 🎉', tags = ['a', 'b'] WHERE id = 1");

            databaseConnection.executeUpdate(
                    "DELETE FROM " + edgeCasesTableName + " WHERE id = 1");

            assertTrue(waitForAvailableRecords(waitTimeForRecords(), TimeUnit.SECONDS));
            SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
            List<SourceRecord> records = sourceRecords.recordsForTopic(getTopicName(config, edgeCasesTableName));
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
            databaseConnection.dropChangeStream(edgeCasesChangeStreamName);
            databaseConnection.dropTable(edgeCasesTableName);
        }
    }
}
