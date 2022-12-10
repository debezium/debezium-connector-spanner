/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.metadata;

import java.util.List;
import java.util.Objects;

import io.debezium.spi.schema.DataCollectionId;

/**
 * Provides the identifier for Spanner DB table
 */
public class TableId implements DataCollectionId {

    private final String tableName;
    private final String id;

    private TableId(String id, String tableName) {
        this.tableName = tableName;
        this.id = id;
    }

    public static TableId getTableId(String tableName) {
        return new TableId(tableName, tableName);
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String identifier() {
        return id;
    }

    @Override
    public List<String> parts() {
        return List.of(tableName);
    }

    @Override
    public List<String> databaseParts() {
        return List.of(tableName);
    }

    @Override
    public List<String> schemaParts() {
        return List.of(tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableId tableId = (TableId) o;
        return Objects.equals(id, tableId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return this.identifier();
    }
}
