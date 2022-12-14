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

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.spanner.schema.mapper.JsonNodeStructValueConvertor;

class JsonNodeStructValueConvertorTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void getBytes() throws JsonProcessingException {
        Assertions.assertEquals(
                ByteBuffer.wrap(Base64.getDecoder().decode("test".getBytes(StandardCharsets.UTF_8))),
                JsonNodeStructValueConvertor.getBytes(OBJECT_MAPPER
                        .readTree("{\"value\": \"test\" }").get("value")));
    }

    @Test
    void getLong() throws JsonProcessingException {
        Assertions.assertEquals(10L,
                JsonNodeStructValueConvertor.getLong(OBJECT_MAPPER.readTree("{\"value\": 10 }").get("value")));
    }

    @Test
    void getDouble() throws JsonProcessingException {
        Assertions.assertEquals(1.123,
                JsonNodeStructValueConvertor.getDouble(OBJECT_MAPPER.readTree("{\"value\": 1.123 }").get("value")));
    }

    @Test
    void getBoolean() throws JsonProcessingException {
        Assertions.assertEquals(true,
                JsonNodeStructValueConvertor.getBoolean(OBJECT_MAPPER.readTree("{\"value\": true }").get("value")));
    }

    @Test
    void getString() throws JsonProcessingException {
        Assertions.assertEquals("test_string",
                JsonNodeStructValueConvertor.getString(OBJECT_MAPPER
                        .readTree("{\"value\": \"test_string\" }").get("value")));
    }

    @Test
    void getList() throws JsonProcessingException {
        Assertions.assertEquals(List.of(1.1, 2.0, 3.4),
                JsonNodeStructValueConvertor.getList(OBJECT_MAPPER
                        .readTree("{\"value\": [1.1, 2, 3.4] }").get("value"), Schema.Type.FLOAT64));
    }
}
