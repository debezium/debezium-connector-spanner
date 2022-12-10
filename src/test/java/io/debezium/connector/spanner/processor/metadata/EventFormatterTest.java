/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor.metadata;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.data.Envelope;

class EventFormatterTest {

    @Test
    void testPrintStruct() {
        EventFormatter eventFormatter = new EventFormatter();
        Schema schema = SchemaBuilder.type(Schema.Type.STRUCT).field("someField", Schema.STRING_SCHEMA).schema();
        Struct struct = new Struct(schema);
        eventFormatter.printStruct(struct);
        assertTrue(eventFormatter.toString().contains("someField"));
    }

    @Test
    void testPrintStructOperationNull() {
        EventFormatter eventFormatter = new EventFormatter();
        Schema structSchema = SchemaBuilder.type(Schema.Type.STRUCT).schema();

        Schema beforeSchema = SchemaBuilder.type(Schema.Type.STRUCT).field("before", structSchema).schema();
        Schema afterSchema = SchemaBuilder.type(Schema.Type.STRUCT).field("after", structSchema).schema();

        Schema schema = SchemaBuilder.type(Schema.Type.STRUCT)
                .field("someField", Schema.STRING_SCHEMA)
                .field("op", Schema.STRING_SCHEMA)
                .field("before", beforeSchema)
                .field("after", afterSchema)
                .schema();

        Struct value = spy(new Struct(schema));
        doReturn("testStructString").when(value).toString();
        doReturn("testStructStringOperation").when(value).getString(Envelope.FieldName.OPERATION);

        eventFormatter.value(value);

        assertTrue(eventFormatter.toString().contains("testStructStringOperation"));
    }

    @Test
    void testPrintStructOperationAddKeyInstanceofStruct() {
        EventFormatter eventFormatter = new EventFormatter();
        Schema structSchema = SchemaBuilder.type(Schema.Type.STRUCT).schema();

        Schema beforeSchema = SchemaBuilder.type(Schema.Type.STRUCT).field("before", structSchema).schema();
        Schema afterSchema = SchemaBuilder.type(Schema.Type.STRUCT).field("after", structSchema).schema();

        Schema schema = SchemaBuilder.type(Schema.Type.STRUCT)
                .field("someField", Schema.STRING_SCHEMA)
                .field("op", Schema.STRING_SCHEMA)
                .field("before", beforeSchema)
                .field("after", afterSchema)
                .schema();

        Struct value = spy(new Struct(schema));
        doReturn("testStructString").when(value).toString();
        doReturn("testStructStringOperation").when(value).getString(Envelope.FieldName.OPERATION);

        eventFormatter.value(value);

        assertTrue(eventFormatter.toString().contains("testStructStringOperation"));
    }

    private static Stream<Arguments> toStringProvider() {
        return Stream.of(
                Arguments.of(new EventFormatter().toString(), ""),
                Arguments.of(new EventFormatter().sourcePosition(new HashMap<>()).toString(),
                        "position: {}"),
                Arguments.of(new EventFormatter().key("Key").toString(),
                        "key: Key"),
                Arguments.of(new EventFormatter().sourcePosition(Map.of("position: {", "42")).toString(),
                        "position: {position: {: 42}"));
    }

    @ParameterizedTest
    @MethodSource("toStringProvider")
    void testToString(Object string, String expected) {
        assertTrue(expected.contains(string.toString()));
    }
}
