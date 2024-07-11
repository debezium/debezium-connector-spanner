/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.schema.mapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.connect.data.Schema;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Utility to parse JsonNode data into various data types
 */
public class JsonNodeStructValueConvertor {

    private JsonNodeStructValueConvertor() {
    }

    public static ByteBuffer getBytes(JsonNode node) {
        if (node.isNull()) {
            return null;
        }
        byte[] bytes = Base64.getDecoder().decode(node.asText().getBytes(StandardCharsets.UTF_8));
        return ByteBuffer.wrap(bytes);
    }

    public static Long getLong(JsonNode node) {
        if (node.isNull()) {
            return null;
        }
        return node.asLong();
    }

    public static Float getFloat(JsonNode node) {
        if (node.isNull()) {
            return null;
        }
        // It is safe to down cast here. Spanner has up casted the float32
        // values to float64 while sending the response.
        return (float) node.asDouble();
    }

    public static Double getDouble(JsonNode node) {
        if (node.isNull()) {
            return null;
        }
        return node.asDouble();
    }

    public static Boolean getBoolean(JsonNode node) {
        if (node.isNull()) {
            return null;
        }
        return node.asBoolean();
    }

    public static String getString(JsonNode node) {
        if (node.isNull()) {
            return null;
        }
        return node.asText();
    }

    public static List<Object> getList(JsonNode node, Schema.Type type) {
        if (node.isNull()) {
            return null;
        }

        if (!node.isArray()) {
            throw new IllegalArgumentException();
        }

        return StreamSupport.stream(node.spliterator(), false)
                .map(elNode -> getValueFromNode(elNode, type))
                .collect(Collectors.toList());
    }

    private static Object getValueFromNode(JsonNode node, Schema.Type type) {
        switch (type) {
            case FLOAT32:
                return getFloat(node);
            case FLOAT64:
                return getDouble(node);
            case STRING:
                return getString(node);
            case INT64:
                return getLong(node);
            case BOOLEAN:
                return getBoolean(node);
            case BYTES:
                return getBytes(node);
            default:
                throw new IllegalArgumentException();
        }
    }
}
