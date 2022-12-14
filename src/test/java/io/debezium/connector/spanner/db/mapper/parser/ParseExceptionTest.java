/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.mapper.parser;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class ParseExceptionTest {

    @Test
    void testConstructor() {
        Exception exception = new Exception();
        assertSame(new ParseException("Json", exception).getCause(), exception);
    }
}
