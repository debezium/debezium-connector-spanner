/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.schema;

import com.google.cloud.spanner.Dialect;
import com.google.common.base.Objects;

/**
 * DTO for Spanner DB column
 */
public class Column {
    private final String name;
    private final ColumnType type;
    private final boolean primaryKey;
    private final long ordinalPosition;

    private final Boolean nullable;

    public Column(String name, ColumnType type, boolean primaryKey, long ordinalPosition, Boolean nullable) {
        this.name = name;
        this.type = type;
        this.primaryKey = primaryKey;
        this.ordinalPosition = ordinalPosition;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public long getOrdinalPosition() {
        return ordinalPosition;
    }

    public boolean isNullable() {
        return nullable;
    }

    public static Column create(String name, String spannerType, boolean primaryKey,
                                long ordinalPosition, boolean nullable, Dialect dialect) {
        return new Column(name, parseColumnType(spannerType, dialect), primaryKey, ordinalPosition,
                nullable);
    }

    private static ColumnType parseColumnType(String spannerType, Dialect dialect) {
        spannerType = spannerType.toUpperCase();
        switch (dialect) {
            case GOOGLE_STANDARD_SQL:
                if ("BOOL".equals(spannerType)) {
                    return new ColumnType(DataType.BOOL);
                }
                if ("INT64".equals(spannerType)) {
                    return new ColumnType(DataType.INT64);
                }
                if ("FLOAT32".equals(spannerType)) {
                    return new ColumnType(DataType.FLOAT32);
                }
                if ("FLOAT64".equals(spannerType)) {
                    return new ColumnType(DataType.FLOAT64);
                }
                if (spannerType.startsWith("STRING")) {
                    return new ColumnType(DataType.STRING);
                }
                if (spannerType.startsWith("BYTES")) {
                    return new ColumnType(DataType.BYTES);
                }
                if ("TIMESTAMP".equals(spannerType)) {
                    return new ColumnType(DataType.TIMESTAMP);
                }
                if ("DATE".equals(spannerType)) {
                    return new ColumnType(DataType.DATE);
                }
                if ("NUMERIC".equals(spannerType)) {
                    return new ColumnType(DataType.NUMERIC);
                }
                if ("JSON".equals(spannerType)) {
                    return new ColumnType(DataType.JSON);
                }
                if (spannerType.startsWith("ARRAY")) {
                    // Substring "ARRAY<xxx>"
                    String spannerArrayType = spannerType.substring(6, spannerType.length() - 1);
                    ColumnType itemType = parseColumnType(spannerArrayType, dialect);
                    return new ColumnType(DataType.ARRAY, itemType);
                }
                throw new IllegalArgumentException("Unknown spanner type " + spannerType);
            case POSTGRESQL:
                if (spannerType.endsWith("[]")) {
                    // Substring "xxx[]"
                    // Must check array type first
                    String spannerArrayType = spannerType.substring(0, spannerType.length() - 2);
                    ColumnType itemType = parseColumnType(spannerArrayType, dialect);
                    return new ColumnType(DataType.ARRAY, itemType);
                }
                if ("BOOLEAN".equals(spannerType)) {
                    return new ColumnType(DataType.BOOL);
                }
                if ("BIGINT".equals(spannerType)) {
                    return new ColumnType(DataType.INT64);
                }
                if ("REAL".equals(spannerType)) {
                    return new ColumnType(DataType.FLOAT32);
                }
                if ("DOUBLE PRECISION".equals(spannerType)) {
                    return new ColumnType(DataType.FLOAT64);
                }
                if (spannerType.startsWith("CHARACTER VARYING") || "TEXT".equals(spannerType)) {
                    return new ColumnType(DataType.STRING);
                }
                if ("BYTEA".equals(spannerType)) {
                    return new ColumnType(DataType.BYTES);
                }
                if ("TIMESTAMP WITH TIME ZONE".equals(spannerType)) {
                    return new ColumnType(DataType.TIMESTAMP);
                }
                if ("DATE".equals(spannerType)) {
                    return new ColumnType(DataType.DATE);
                }
                if (spannerType.startsWith("NUMERIC")) {
                    return new ColumnType(DataType.NUMERIC);
                }
                if ("SPANNER.COMMIT_TIMESTAMP".equals(spannerType)) {
                    return new ColumnType(DataType.TIMESTAMP);
                }
                if ("JSONB".equals(spannerType)) {
                    return new ColumnType(DataType.JSON);
                }
                throw new IllegalArgumentException("Unknown spanner type " + spannerType);
            default:
                throw new IllegalArgumentException("Unrecognized dialect: " + dialect.name());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Column column = (Column) o;
        return primaryKey == column.primaryKey && ordinalPosition == column.ordinalPosition
                && Objects.equal(name, column.name) && Objects.equal(type, column.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, type, primaryKey, ordinalPosition);
    }
}
