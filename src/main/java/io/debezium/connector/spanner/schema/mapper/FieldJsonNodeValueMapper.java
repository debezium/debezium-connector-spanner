/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.schema.mapper;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Maps field values from JsonNode, based on field schema
 */
public class FieldJsonNodeValueMapper {

    private FieldJsonNodeValueMapper() {
    }

    public static Object getValue(Field field, JsonNode node) {
        Schema.Type type = field.schema().type();
        switch (type) {
            case FLOAT64:
                return JsonNodeStructValueConvertor.getDouble(node);
            case STRING:
                return JsonNodeStructValueConvertor.getString(node);
            case INT64:
                return JsonNodeStructValueConvertor.getLong(node);
            case BOOLEAN:
                return JsonNodeStructValueConvertor.getBoolean(node);
            case BYTES:
                return JsonNodeStructValueConvertor.getBytes(node);
            case ARRAY:
                return JsonNodeStructValueConvertor.getList(node, field.schema().valueSchema().type());
            default:
                throw new IllegalArgumentException("unsupported field type: " + type);
        }
    }
}
