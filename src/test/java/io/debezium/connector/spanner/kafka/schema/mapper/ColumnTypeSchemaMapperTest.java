/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.schema.mapper;

import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.connector.spanner.db.mapper.parser.ColumnTypeParser;
import io.debezium.connector.spanner.schema.mapper.ColumnTypeSchemaMapper;

class ColumnTypeSchemaMapperTest {

    private static Stream<Arguments> schemaProvider() {
        return Stream.of(
                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser
                                .parse("{\"array_element_type\":{\"code\":\"BOOL\"},\"code\":\"ARRAY\"}"), true),
                        SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).optional()),

                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"STRING\"}"), true),
                        Schema.OPTIONAL_STRING_SCHEMA),
                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"NUMERIC\"}"), true),
                        Schema.OPTIONAL_STRING_SCHEMA),
                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"DATE\"}"), true),
                        Schema.OPTIONAL_STRING_SCHEMA),
                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"JSON\"}"), true),
                        Schema.OPTIONAL_STRING_SCHEMA),
                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"TIMESTAMP\"}"), true),
                        Schema.OPTIONAL_STRING_SCHEMA),

                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"INT64\"}"), true),
                        Schema.OPTIONAL_INT64_SCHEMA),

                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"BOOL\"}"), true),
                        Schema.OPTIONAL_BOOLEAN_SCHEMA),

                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"FLOAT32\"}"), true),
                        Schema.OPTIONAL_FLOAT32_SCHEMA),

                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"FLOAT64\"}"), true),
                        Schema.OPTIONAL_FLOAT64_SCHEMA),

                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"BYTES\"}"), true),
                        SchemaBuilder.bytes().optional()),

                Arguments.of(
                        ColumnTypeSchemaMapper.getSchema(ColumnTypeParser.parse("{\"code\":\"TOKENLIST\"}"), true),
                        Schema.OPTIONAL_STRING_SCHEMA));
    }

    @ParameterizedTest
    @MethodSource("schemaProvider")
    void parse(Schema schema, Schema expected) {
        Assertions.assertEquals(expected.type(), schema.type());
        Assertions.assertEquals(expected.isOptional(), schema.isOptional());
        Assertions.assertEquals(expected.defaultValue(), schema.defaultValue());
        Assertions.assertEquals(expected.name(), schema.name());
        Assertions.assertEquals(expected.version(), schema.version());
        Assertions.assertEquals(expected.doc(), schema.doc());
        Assertions.assertEquals(expected.parameters(), schema.parameters());
        Assertions.assertEquals(expected.schema().type(), schema.schema().type());
    }
}
