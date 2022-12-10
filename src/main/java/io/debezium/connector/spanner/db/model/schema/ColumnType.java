/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.schema;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO for Spanner DB column type
 */
public class ColumnType {
    @JsonProperty("code")
    private DataType type;
    @JsonProperty("array_element_type")
    private ColumnType arrayElementType;

    public ColumnType() {
    }

    public ColumnType(DataType type) {
        this.type = type;
    }

    public ColumnType(DataType type, ColumnType arrayElementType) {
        this.type = type;
        this.arrayElementType = arrayElementType;
    }

    public DataType getType() {
        return type;
    }

    public ColumnType getArrayElementType() {
        return arrayElementType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnType that = (ColumnType) o;
        return type == that.type && Objects.equals(arrayElementType, that.arrayElementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, arrayElementType);
    }

    @Override
    public String toString() {
        return "ColumnType{"
                + "type='"
                + type
                + ", arrayElementType="
                + arrayElementType
                + '}';
    }
}
