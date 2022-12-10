/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.schema;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Contains schema for all table columns
 */
public class TableSchema {
    private final String name;
    private final Map<String, Column> columns;

    public TableSchema(String name, List<Column> columns) {
        this.name = name;
        this.columns = columns.stream().collect(Collectors.toMap(Column::getName, Function.identity()));
    }

    public String getName() {
        return name;
    }

    public List<Column> keyColumns() {
        return columns.values().stream()
                .filter(Column::isPrimaryKey)
                .collect(Collectors.toList());
    }

    public List<Column> columns() {
        return columns.values().stream()
                .sorted(Comparator.comparing(Column::getOrdinalPosition))
                .collect(Collectors.toList());
    }

    public Column getColumn(String name) {
        return columns.get(name);
    }
}
