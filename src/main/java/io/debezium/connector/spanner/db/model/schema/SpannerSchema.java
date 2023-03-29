/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.cloud.spanner.Dialect;

import io.debezium.connector.spanner.db.metadata.TableId;

/**
 * Contains schema for each DB table
 */
public class SpannerSchema {
    private final Map<TableId, TableSchema> tables;

    public SpannerSchema(Map<TableId, TableSchema> tables) {
        this.tables = tables;
    }

    public TableSchema getTable(TableId tableId) {
        return this.tables.get(tableId);
    }

    public static SpannerSchemaBuilder builder() {
        return new SpannerSchemaBuilder();
    }

    public Set<TableId> getAllTables() {
        return tables.keySet();
    }

    public static class SpannerSchemaBuilder {
        private final Map<String, List<Column>> tableMap = new HashMap<>();
        private final Map<String, List<String>> tableToPrimaryColumnMap = new HashMap<>();

        private SpannerSchemaBuilder() {
        }

        public void addColumn(String tableName, String columnName, String type, long ordinalPosition, boolean nullable, Dialect dialect) {
            List<Column> columns;
            if (tableMap.containsKey(tableName)) {
                columns = tableMap.get(tableName);
            }
            else {
                columns = new ArrayList<>();
                tableMap.put(tableName, columns);
            }
            columns.add(Column.create(columnName, type, tableToPrimaryColumnMap.get(tableName).contains(columnName), ordinalPosition, nullable, dialect));
        }

        public void addPrimaryColumn(String tableName, String columnName) {
            List<String> primaryColumns;
            if (tableToPrimaryColumnMap.containsKey(tableName)) {
                primaryColumns = tableToPrimaryColumnMap.get(tableName);
            }
            else {
                primaryColumns = new ArrayList<>();
                tableToPrimaryColumnMap.put(tableName, primaryColumns);
            }
            primaryColumns.add(columnName);
        }

        public SpannerSchema build() {
            Map<TableId, TableSchema> tables = new HashMap<>();

            tableMap.forEach((tableName, columns) -> {
                TableId tableId = TableId.getTableId(tableName);
                TableSchema table = new TableSchema(tableName, columns);
                tables.put(tableId, table);
            });

            return new SpannerSchema(tables);
        }
    }
}
