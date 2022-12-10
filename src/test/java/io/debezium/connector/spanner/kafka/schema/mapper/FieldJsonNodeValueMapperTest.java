/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.schema.mapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.spanner.db.mapper.parser.ColumnTypeParser;
import io.debezium.connector.spanner.schema.mapper.ColumnTypeSchemaMapper;
import io.debezium.connector.spanner.schema.mapper.FieldJsonNodeValueMapper;

class FieldJsonNodeValueMapperTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static Stream<Arguments> schemaProvider() throws JsonProcessingException {
        return Stream.of(
                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser
                                .parse("{\"array_element_type\":{\"code\":\"FLOAT64\"},\"code\":\"ARRAY\"}"), true),
                        OBJECT_MAPPER.readTree("{\"value\": [1.1, 2, 3.4] }").get("value"),
                        List.of(1.1, 2.0, 3.4)),
                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"STRING\"}"), true),
                        OBJECT_MAPPER.readTree("{\"value\": \"test_string\" }").get("value"),
                        "test_string"),
                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"INT64\"}"), true),
                        OBJECT_MAPPER.readTree("{\"value\": 10 }").get("value"),
                        10L),
                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"BOOL\"}"), true),
                        OBJECT_MAPPER.readTree("{\"value\": true }").get("value"),
                        true),
                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"FLOAT64\"}"), true),
                        OBJECT_MAPPER.readTree("{\"value\": 1.123 }").get("value"),
                        1.123),
                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"BYTES\"}"), true),
                        OBJECT_MAPPER.readTree("{\"value\": \"test\" }").get("value"),
                        ByteBuffer.wrap(Base64.getDecoder().decode("test".getBytes(StandardCharsets.UTF_8)))));
    }

    @ParameterizedTest
    @MethodSource("schemaProvider")
    void parse(Schema schema, JsonNode objectNode, Object expected) {
        Field field = new Field("test", 0, schema);
        Object value = FieldJsonNodeValueMapper.getValue(field, objectNode);
        Assertions.assertEquals(expected, value);
    }
}
