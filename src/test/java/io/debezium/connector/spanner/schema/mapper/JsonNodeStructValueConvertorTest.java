/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.schema.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;

class JsonNodeStructValueConvertorTest {

    @Test
    void testGetBytesNotNull() {
        assertNotNull(JsonNodeStructValueConvertor.getBytes(MissingNode.getInstance()));
    }

    @Test
    void testGetBytes() {
        ByteBuffer bytes = JsonNodeStructValueConvertor.getBytes(new ObjectMapper().createObjectNode());
        assertNotNull(bytes);
    }

    @Test
    void testGetLong() {
        assertEquals(0L, JsonNodeStructValueConvertor.getLong(MissingNode.getInstance()).longValue());
        assertEquals(42L,
                JsonNodeStructValueConvertor.getLong(new BigIntegerNode(BigInteger.valueOf(42L))).longValue());
        assertEquals(0L, JsonNodeStructValueConvertor.getLong(BooleanNode.getFalse()).longValue());
        assertNull(JsonNodeStructValueConvertor.getLong(NullNode.getInstance()));
    }

    @Test
    void testGetDouble() {
        assertEquals(0.0d, JsonNodeStructValueConvertor.getDouble(MissingNode.getInstance()).doubleValue());
        assertEquals(42.0d,
                JsonNodeStructValueConvertor.getDouble(new BigIntegerNode(BigInteger.valueOf(42L))).doubleValue());
        assertEquals(0.0d, JsonNodeStructValueConvertor.getDouble(BooleanNode.getFalse()).doubleValue());
        assertNull(JsonNodeStructValueConvertor.getDouble(NullNode.getInstance()));
    }

    @Test
    void testGetBoolean() {
        assertFalse(JsonNodeStructValueConvertor.getBoolean(MissingNode.getInstance()));
        assertTrue(JsonNodeStructValueConvertor.getBoolean(new BigIntegerNode(BigInteger.valueOf(42L))));
        assertFalse(JsonNodeStructValueConvertor.getBoolean(BooleanNode.getFalse()));
        assertNull(JsonNodeStructValueConvertor.getBoolean(NullNode.getInstance()));
    }

    @Test
    void testGetString() throws UnsupportedEncodingException {
        assertEquals("", JsonNodeStructValueConvertor.getString(MissingNode.getInstance()));
        assertEquals("dGVzdA==",
                JsonNodeStructValueConvertor.getString(new BinaryNode("test".getBytes("UTF-8"))));
        assertNull(JsonNodeStructValueConvertor.getString(NullNode.getInstance()));
    }

    @Test
    void testGetList() {
        assertThrows(IllegalArgumentException.class,
                () -> JsonNodeStructValueConvertor.getList(MissingNode.getInstance(), Schema.Type.INT8));
        assertTrue(JsonNodeStructValueConvertor
                .getList(new ArrayNode(JsonNodeFactory.withExactBigDecimals(true)), Schema.Type.INT8)
                .isEmpty());
        assertNull(JsonNodeStructValueConvertor.getList(NullNode.getInstance(), Schema.Type.INT8));
        assertThrows(IllegalArgumentException.class,
                () -> JsonNodeStructValueConvertor.getList(MissingNode.getInstance(), Schema.Type.INT16));
        assertThrows(IllegalArgumentException.class,
                () -> JsonNodeStructValueConvertor.getList(MissingNode.getInstance(), Schema.Type.INT32));
        assertThrows(IllegalArgumentException.class,
                () -> JsonNodeStructValueConvertor.getList(MissingNode.getInstance(), Schema.Type.INT64));
    }

    @Test
    void testGetListThrows() {
        ArrayNode arrayNode = new ArrayNode(JsonNodeFactory.withExactBigDecimals(true));
        arrayNode.add(MissingNode.getInstance());
        assertThrows(IllegalArgumentException.class,
                () -> JsonNodeStructValueConvertor.getList(arrayNode, Schema.Type.INT8));
    }
}
