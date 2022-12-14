/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.schema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Holds the schema of all database tables tracked by a Change Stream
 */
public class ChangeStreamSchema {
    private final String name;
    private final boolean allTables;

    private final Map<String, Table> tables;

    public ChangeStreamSchema(String name, boolean allTables, List<Table> tables) {
        this.name = name;
        this.allTables = allTables;
        this.tables = tables.stream().collect(Collectors.toMap(Table::getName, Function.identity()));
    }

    public String getName() {
        return name;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Set<String> getTables() {
        if (allTables) {
            return Set.of();
        }
        return tables.keySet();
    }

    public Table getTable(String name) {
        if (allTables) {
            return new Table(name, true);
        }
        return tables.get(name);
    }

    public boolean isWatchedAllTables() {
        return this.allTables;
    }

    public static class Builder {
        private String name;
        private boolean allTables = false;
        private final Map<String, Boolean> tableMap = new HashMap<>();
        private final Map<String, Set<String>> tableColumnMap = new HashMap<>();

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder allTables(boolean allTables) {
            this.allTables = allTables;
            return this;
        }

        public Builder table(String tableName, boolean allColumns) {
            tableMap.put(tableName, allColumns);
            tableColumnMap.putIfAbsent(tableName, new HashSet<>());
            return this;
        }

        public Builder column(String tableName, String columnName) {
            tableColumnMap.get(tableName).add(columnName);
            return this;
        }

        public ChangeStreamSchema build() {
            List<Table> tables = tableMap.keySet().stream()
                    .map(tableName -> new Table(tableName, tableMap.get(tableName), tableColumnMap.get(tableName)))
                    .collect(Collectors.toList());
            return new ChangeStreamSchema(name, allTables, tables);
        }
    }

    public static class Table {
        private final String name;
        private final boolean allColumns;
        private final Set<String> columns;

        public Table(String name, boolean allColumns) {
            this.name = name;
            this.allColumns = allColumns;
            this.columns = Set.of();
        }

        public Table(String name, boolean allColumns, Set<String> columns) {
            this.name = name;
            this.allColumns = allColumns;
            this.columns = columns;
        }

        public String getName() {
            return name;
        }

        public boolean hasColumn(String column) {
            if (allColumns) {
                return true;
            }
            return columns.contains(column);
        }
    }
}
