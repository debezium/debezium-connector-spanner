/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.debezium.connector.spanner.db.model.schema.Column;
import io.debezium.connector.spanner.db.model.schema.SpannerSchema;
import io.debezium.connector.spanner.db.model.schema.TableSchema;

public class SchemaMerger {

    private SchemaMerger() {
    }

    public static SpannerSchema merge(SpannerSchema previousSchema, SpannerSchema newSchema) {
        Map<TableId, TableSchema> tables = new HashMap<>();

        newSchema.getAllTables().forEach(tableId -> {
            TableSchema oldTableSchema = previousSchema.getTable(tableId);
            if (oldTableSchema == null) {
                tables.put(tableId, newSchema.getTable(tableId));
            }
            else {
                TableSchema mergedTable = mergeTable(oldTableSchema, newSchema.getTable(tableId));
                tables.put(tableId, mergedTable);
            }
        });
        return new SpannerSchema(tables);
    }

    private static TableSchema mergeTable(TableSchema previousTableSchema, TableSchema newTableSchema) {
        // collect columns from new schema
        Map<String, Column> columnMap = newTableSchema.columns().stream()
                .collect(Collectors.toMap(Column::getName, Function.identity()));

        List<Column> newColumns = new ArrayList<>(columnMap.values());

        // add not existed columns from previous schema
        newColumns.addAll(previousTableSchema.columns().stream()
                .filter(column -> !columnMap.containsKey(column.getName()))
                .collect(Collectors.toList()));

        return new TableSchema(newTableSchema.getName(), newColumns);
    }
}
