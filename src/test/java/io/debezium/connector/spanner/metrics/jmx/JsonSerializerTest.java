/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.jmx;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class JsonSerializerTest {

    @Test
    void testWriteValueAsString() {
        assertEquals("\"Obj\"", new JsonSerializer().writeValueAsString("Obj"));
        assertEquals("123", new JsonSerializer().writeValueAsString(123));
    }
}
