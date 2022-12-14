/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.schema;

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

    public static Column create(String name, String spannerType, boolean primaryKey, long ordinalPosition, boolean nullable) {
        return new Column(name, parseColumnType(spannerType), primaryKey, ordinalPosition, nullable);
    }

    private static ColumnType parseColumnType(String spannerType) {
        spannerType = spannerType.toUpperCase();
        if ("BOOL".equals(spannerType)) {
            return new ColumnType(DataType.BOOL);
        }
        if ("INT64".equals(spannerType)) {
            return new ColumnType(DataType.INT64);
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
            ColumnType itemType = parseColumnType(spannerArrayType);
            return new ColumnType(DataType.ARRAY, itemType);
        }
        throw new IllegalArgumentException("Unknown spanner type " + spannerType);
    }

}