/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import java.util.Collection;
import java.util.stream.Collectors;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;

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
                    .to(streamName)
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

    public boolean isPostgres() {
        return this.databaseClient.getDialect() == Dialect.POSTGRESQL;
    }
}
