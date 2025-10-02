/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.schema.mapper;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.spanner.db.model.schema.ColumnType;
import io.debezium.connector.spanner.db.model.schema.DataType;

/**
 * Generates Kafka Connect Schema for Spanner data types
 */
public class ColumnTypeSchemaMapper {
    private ColumnTypeSchemaMapper() {
    }

    public static Schema getSchema(ColumnType columnType, boolean optional) {
        switch (columnType.getType()) {
            case STRING:
            case NUMERIC:
            case DATE:
            case JSON:
            case TIMESTAMP:
            case TOKENLIST:
                return optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
            case INT64:
                return optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
            case BOOL:
                return optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
            case FLOAT32:
                return optional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA;
            case FLOAT64:
                return optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
            case ARRAY:
                return getArraySchema(columnType, optional);
            case BYTES:
                return optional ? SchemaBuilder.bytes().optional() : SchemaBuilder.bytes();
            case STRUCT:
            default:
                throw new IllegalArgumentException();
        }
    }

    private static Schema getArraySchema(ColumnType columnType, boolean optional) {
        Schema arrayElementSchema = getArrayElementSchema(columnType.getArrayElementType().getType(), true);

        return optional ? SchemaBuilder.array(arrayElementSchema).optional() : SchemaBuilder.array(arrayElementSchema);
    }

    private static Schema getArrayElementSchema(DataType type, boolean optional) {
        switch (type) {
            case STRING:
            case NUMERIC:
            case DATE:
            case JSON:
            case TIMESTAMP:
            case TOKENLIST:
                return optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
            case INT64:
                return optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
            case BOOL:
                return optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
            case FLOAT32:
                return optional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA;
            case FLOAT64:
                return optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
            case BYTES:
                return optional ? SchemaBuilder.bytes().optional() : SchemaBuilder.bytes();
            default:
                throw new IllegalArgumentException();
        }
    }
}
