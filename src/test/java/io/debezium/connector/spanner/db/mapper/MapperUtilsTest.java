/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.MissingNode;

import io.debezium.connector.spanner.db.mapper.parser.ParseException;

class MapperUtilsTest {

    @Test
    void testGetJsonNode() {
        assertThrows(ParseException.class, () -> MapperUtils.getJsonNode("Json"));
        assertEquals(0, MapperUtils.getJsonNode(null).size());
        assertEquals("42", MapperUtils.getJsonNode("42").toPrettyString());
        assertTrue(MapperUtils.getJsonNode("") instanceof MissingNode);
        assertThrows(ParseException.class, () -> MapperUtils.getJsonNode("42Json"));
        assertEquals("4242", MapperUtils.getJsonNode("4242").toPrettyString());
    }
}
