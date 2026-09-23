/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;

import io.debezium.connector.spanner.db.model.ChangeStreamOptions;
import io.debezium.connector.spanner.db.model.schema.ChangeStreamSchema;
import io.debezium.connector.spanner.db.model.schema.SpannerSchema;

/**
 * Provides functionality to read Spanner DB table and stream schema
 */
public class SchemaDao {

    private final DatabaseClient databaseClient;

    public SchemaDao(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public SpannerSchema getSchema(Timestamp timestamp) {
        return getSchema(timestamp, null);
    }

    public SpannerSchema getSchema(Timestamp timestamp, Collection<String> tables) {
        SpannerSchema.SpannerSchemaBuilder builder = SpannerSchema.builder();
        try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction(TimestampBound.ofReadTimestamp(timestamp))) {
            ResultSet resultSet = readColumnsInfo(tx, tables);
            ResultSet primaryColumnsResultSet = readPrimaryColumns(tx, tables);

            while (primaryColumnsResultSet.next()) {
                String tableName = primaryColumnsResultSet.getString(0);
                String columnName = primaryColumnsResultSet.getString(1);
                builder.addPrimaryColumn(tableName, columnName);
            }

            while (resultSet.next()) {
                String tableName = resultSet.getString(0);
                String columnName = resultSet.getString(1);
                String type = resultSet.getString(2);
                long ordinalPosition = resultSet.getLong(3);
                boolean nullable = resultSet.getBoolean(4);
                builder.addColumn(tableName, columnName, type, ordinalPosition, nullable, this.databaseClient.getDialect());
            }
        }
        return builder.build();
    }

    public ChangeStreamSchema getStream(Timestamp timestamp, String streamName) {
        ChangeStreamSchema.Builder builder = ChangeStreamSchema.builder()
                .name(streamName);
        boolean exist = false;
        try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction(TimestampBound.ofReadTimestamp(timestamp))) {
            ResultSet resultSet = readChangeStreamInfo(tx, streamName);

            while (resultSet.next()) {
                exist = true;
                boolean allTables = resultSet.getBoolean(0);
                builder.allTables(allTables);
                if (!allTables) {
                    String tableName = resultSet.getString(1);
                    boolean allColumns = resultSet.getBoolean(2);
                    builder.table(tableName, allColumns);

                    if (!allColumns) {
                        String columnName = resultSet.getString(3);
                        builder.column(tableName, columnName);
                    }
                }
            }
        }
        return exist ? builder.build() : null;
    }

    public ChangeStreamOptions getChangeStreamOptions(String streamName) {
        boolean mutableKeyRange = false;
        boolean perPlacementTvf = false;
        try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
            ResultSet resultSet = readChangeStreamOptions(tx, streamName);

            while (resultSet.next()) {
                String optionName = resultSet.getString(0);
                String optionValue = resultSet.getString(1);
                if ("partition_mode".equalsIgnoreCase(optionName)) {
                    mutableKeyRange = "MUTABLE_KEY_RANGE".equalsIgnoreCase(optionValue);
                }
                else if ("per_placement_tvf".equalsIgnoreCase(optionName)) {
                    perPlacementTvf = Boolean.parseBoolean(optionValue);
                }
            }
        }
        return new ChangeStreamOptions(mutableKeyRange, perPlacementTvf);
    }

    public boolean isMutableKeyRangeChangeStream(String streamName) {
        return getChangeStreamOptions(streamName).isMutableKeyRange();
    }

    public boolean isPerPlacementTvfChangeStream(String streamName) {
        return getChangeStreamOptions(streamName).isPerPlacementTvf();
    }

    /**
     * Validates a caller-supplied list of per-placement read table-valued function (TVF) names
     * (e.g. {@code READ_Foo_US}, {@code READ_Foo_EU}) configured via
     * {@code gcp.spanner.placement.tvf.names} for {@code changeStreamName}.
     *
     * <p>Spanner does not expose an information_schema relation that maps a change stream to its
     * per-placement TVFs directly, so, similar to the check the Apache Beam Spanner change
     * streams connector performs before unioning a list of TVFs (see
     * {@code SpannerIO#checkTvfExistence}), this queries {@code information_schema.routines} -
     * which for change streams lists their auto-generated read functions - to confirm each
     * configured name actually exists as a function. Unlike the Beam implementation (which only
     * checks existence), this additionally verifies that every configured TVF is actually
     * associated with {@code changeStreamName}, based on the naming convention Spanner uses for
     * change stream read functions: {@code READ_<changeStreamName>[_<placement>]} for GoogleSQL,
     * and {@code read_proto_bytes_<changeStreamName>[_<placement>]} /
     * {@code read_json_<changeStreamName>[_<placement>]} for PostgreSQL (depending on whether the
     * stream is mutable). This guards against a misconfigured list silently reading a different
     * change stream's placement data.
     *
     * @throws IllegalArgumentException if the stream does not have {@code per_placement_tvf}
     *     enabled, or if any configured TVF does not exist or is not associated with the stream
     */
    public void validatePlacementTvfNames(String streamName, List<String> placementTvfNames) {
        if (placementTvfNames == null || placementTvfNames.isEmpty()) {
            return;
        }
        validatePlacementTvfNames(streamName, placementTvfNames, getChangeStreamOptions(streamName));
    }

    public void validatePlacementTvfNames(String streamName, List<String> placementTvfNames, ChangeStreamOptions options) {
        if (placementTvfNames == null || placementTvfNames.isEmpty()) {
            return;
        }

        if (!options.isPerPlacementTvf()) {
            throw new IllegalArgumentException("Configured placement TVF names " + placementTvfNames
                    + " for change stream '" + streamName + "', but this change stream does not have "
                    + "the 'per_placement_tvf' option enabled.");
        }

        Set<String> existingRoutineNames = readExistingRoutineNames(placementTvfNames);
        String expectedPrefix = expectedTvfPrefix(streamName, options.isMutableKeyRange());

        for (String tvfName : placementTvfNames) {
            String bareName = isPostgres() ? PostgresIdentifier.routineName(tvfName) : tvfName;
            if (!existingRoutineNames.contains(bareName)) {
                throw new IllegalArgumentException("Configured placement TVF '" + tvfName
                        + "' was not found among the database's routines: " + existingRoutineNames);
            }
            if (!isAssociatedWithChangeStream(bareName, expectedPrefix)) {
                throw new IllegalArgumentException("Configured placement TVF '" + tvfName
                        + "' does not appear to be associated with change stream '" + streamName
                        + "' (expected a name matching '" + expectedPrefix + "' or '" + expectedPrefix + "_<placement>')");
            }
        }
    }

    /**
     * Builds the expected read-function name prefix for {@code streamName}, following the same
     * naming convention used to build the default TVF name in
     * {@link ChangeStreamDao#streamQuery(String, String, Timestamp, Timestamp, long)}.
     */
    private String expectedTvfPrefix(String streamName, boolean mutableKeyRange) {
        if (isPostgres()) {
            String base = mutableKeyRange ? "read_proto_bytes_" : "read_json_";
            return (base + streamName).toLowerCase(Locale.ROOT);
        }
        return "READ_" + streamName;
    }

    private boolean isAssociatedWithChangeStream(String bareTvfName, String expectedPrefix) {
        String candidate = isPostgres() ? bareTvfName.toLowerCase(Locale.ROOT) : bareTvfName;
        return candidate.equals(expectedPrefix) || candidate.startsWith(expectedPrefix + "_");
    }

    /**
     * Queries {@code information_schema.routines} for the subset of {@code tvfNames} that exist
     * as table-valued functions, mirroring the existence check performed by the Apache Beam
     * Spanner change streams connector (see {@code SpannerIO#checkTvfExistence}).
     */
    private Set<String> readExistingRoutineNames(List<String> tvfNames) {
        Set<String> found = new HashSet<>();
        try (ReadOnlyTransaction tx = databaseClient.readOnlyTransaction()) {
            ResultSet resultSet = tx.executeQuery(buildRoutineExistenceStatement(tvfNames));
            while (resultSet.next()) {
                found.add(resultSet.getString(0));
            }
        }
        return found;
    }

    private Statement buildRoutineExistenceStatement(List<String> tvfNames) {
        StringBuilder sql = new StringBuilder(
                "SELECT routine_name FROM information_schema.routines WHERE UPPER(routine_type) LIKE '%FUNCTION' AND routine_name IN (");
        for (int i = 0; i < tvfNames.size(); i++) {
            sql.append(isPostgres() ? "$" + (i + 1) : "@p" + i);
            if (i < tvfNames.size() - 1) {
                sql.append(", ");
            }
        }
        sql.append(")");

        Statement.Builder builder = Statement.newBuilder(sql.toString());
        for (int i = 0; i < tvfNames.size(); i++) {
            String bareName = isPostgres() ? PostgresIdentifier.routineName(tvfNames.get(i)) : tvfNames.get(i);
            builder.bind(isPostgres() ? "p" + (i + 1) : "p" + i).to(bareName);
        }
        return builder.build();
    }

    private ResultSet readColumnsInfo(ReadOnlyTransaction tx, Collection<String> tables) {
        Statement statement;
        if (isPostgres()) {
            statement = Statement.newBuilder("SELECT" +
                    "  table_name," +
                    "  column_name," +
                    "  spanner_type," +
                    "  ordinal_position," +
                    "  CASE WHEN is_nullable = 'YES' THEN TRUE ELSE FALSE END AS is_nullable\n" +
                    "FROM" +
                    "  information_schema.COLUMNS \n" +
                    "WHERE" +
                    "  table_schema = 'public'" +
                    (tables == null ? ""
                            : " AND table_name = ANY(Array[" + tables.stream().map(s -> "'" + s + "'")
                                    .collect(Collectors.joining(",")) + "])"))
                    .build();
        }
        else {
            statement = Statement.newBuilder("SELECT" +
                    "  table_name," +
                    "  column_name," +
                    "  spanner_type," +
                    "  ordinal_position," +
                    "  IF(is_nullable = 'YES', true, false) AS is_nullable\n" +
                    "FROM" +
                    "  information_schema.COLUMNS \n" +
                    "WHERE" +
                    "  table_catalog = ''" +
                    "  AND table_schema = ''" +
                    (tables == null ? "" : "  AND table_name in UNNEST(@tables)"))
                    .bind("tables")
                    .toStringArray(tables)
                    .build();
        }
        return tx.executeQuery(statement);
    }

    private ResultSet readPrimaryColumns(ReadOnlyTransaction tx, Collection<String> tables) {
        Statement statement;
        if (isPostgres()) {
            statement = Statement.newBuilder("SELECT" +
                    "  table_name," +
                    "  column_name\n" +
                    "FROM" +
                    "  information_schema.index_columns \n" +
                    "WHERE" +
                    "   index_name = 'PRIMARY_KEY'" +
                    (tables == null ? ""
                            : "  AND table_name = ANY(Array[" + tables.stream().map(s -> "'" + s + "'")
                                    .collect(Collectors.joining(",")) + "])"))
                    .build();
        }
        else {
            statement = Statement.newBuilder("SELECT" +
                    "  table_name," +
                    "  column_name\n" +
                    "FROM" +
                    "  information_schema.index_columns \n" +
                    "WHERE" +
                    "   index_name = 'PRIMARY_KEY'" +
                    (tables == null ? "" : "  AND table_name in UNNEST(@tables)"))
                    .bind("tables")
                    .toStringArray(tables)
                    .build();
        }
        return tx.executeQuery(statement);
    }

    private ResultSet readChangeStreamInfo(ReadOnlyTransaction tx, String streamName) {
        Statement statement;
        if (isPostgres()) {
            statement = Statement.newBuilder("select" +
                    "  CASE WHEN cs.all = 'YES' then true else false end AS all," +
                    "  cst.table_name," +
                    "  CASE WHEN cst.all_columns = 'YES' then true else false end AS all_columns," +
                    "  csc.column_name\n" +
                    "from" +
                    "  information_schema.change_streams cs\n" +
                    "left join" +
                    "  information_schema.change_stream_tables cst\n" +
                    "on" +
                    "  cst.change_stream_name = cs.change_stream_name\n" +
                    "left join" +
                    "  information_schema.change_stream_columns csc\n" +
                    "on" +
                    "  csc.change_stream_name = cs.change_stream_name\n" +
                    "  and csc.table_name = cst.table_name\n" +
                    "where cs.change_stream_name = $1")
                    .bind("p1")
                    .to(normalizeIdentifier(streamName))
                    .build();
        }
        else {
            statement = Statement.newBuilder("select" +
                    "  cs.all," +
                    "  cst.table_name," +
                    "  cst.all_columns," +
                    "  csc.column_name\n" +
                    "from" +
                    "  information_schema.change_streams cs\n" +
                    "left join" +
                    "  information_schema.change_stream_tables cst\n" +
                    "on" +
                    "  cst.change_stream_name = cs.change_stream_name\n" +
                    "left join" +
                    "  information_schema.change_stream_columns csc\n" +
                    "on" +
                    "  csc.change_stream_name = cs.change_stream_name\n" +
                    "  and csc.table_name = cst.table_name\n" +
                    "where cs.change_stream_name = @streamName")
                    .bind("streamName")
                    .to(streamName)
                    .build();
        }
        return tx.executeQuery(statement);
    }

    private ResultSet readChangeStreamOptions(ReadOnlyTransaction tx, String streamName) {
        Statement statement;
        if (isPostgres()) {
            statement = Statement.newBuilder("select" +
                    "  option_name," +
                    "  option_value\n" +
                    "from" +
                    "  information_schema.change_stream_options\n" +
                    "where change_stream_name = $1")
                    .bind("p1")
                    .to(normalizeIdentifier(streamName))
                    .build();
        }
        else {
            statement = Statement.newBuilder("select" +
                    "  option_name," +
                    "  option_value\n" +
                    "from" +
                    "  information_schema.change_stream_options\n" +
                    "where change_stream_name = @streamName")
                    .bind("streamName")
                    .to(streamName)
                    .build();
        }
        return tx.executeQuery(statement);
    }

    public boolean isPostgres() {
        return this.databaseClient.getDialect() == Dialect.POSTGRESQL;
    }

    /**
     * PostgreSQL-dialect Spanner databases fold unquoted identifiers (e.g. change stream names) to
     * lowercase, same as standard PostgreSQL. Callers configure the change stream name as it was
     * written in DDL (typically unquoted, e.g. {@code MyChangeStream}), so it must be normalized to
     * match what's actually stored in {@code information_schema} before it's used in a lookup.
     */
    private String normalizeIdentifier(String identifier) {
        return isPostgres() ? identifier.toLowerCase(Locale.ROOT) : identifier;
    }
}
