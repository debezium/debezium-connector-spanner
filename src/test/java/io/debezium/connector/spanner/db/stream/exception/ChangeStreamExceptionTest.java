/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class ChangeStreamExceptionTest {

    @Test
    void testExceptionConstructor() {
        Exception exception = new Exception();
        ChangeStreamException actualChangeStreamException = new ChangeStreamException(exception);
        Throwable cause = actualChangeStreamException.getCause();
        assertSame(exception, cause);
        Throwable[] suppressed = actualChangeStreamException.getSuppressed();
        assertEquals(0, suppressed.length);
        assertEquals("java.lang.Exception", actualChangeStreamException.getLocalizedMessage());
        assertEquals("java.lang.Exception", actualChangeStreamException.getMessage());
        assertNull(cause.getLocalizedMessage());
        assertNull(cause.getCause());
        assertNull(cause.getMessage());
        assertSame(suppressed, cause.getSuppressed());
    }

    @Test
    void testStringConstructor() {
        ChangeStreamException actualChangeStreamException = new ChangeStreamException("Msg");
        assertNull(actualChangeStreamException.getCause());
        assertEquals(0, actualChangeStreamException.getSuppressed().length);
        assertEquals("Msg", actualChangeStreamException.getMessage());
        assertEquals("Msg", actualChangeStreamException.getLocalizedMessage());
    }

    @Test
    void testStringExceptionConstructor() {
        Exception exception = new Exception();
        ChangeStreamException actualChangeStreamException = new ChangeStreamException("Msg", exception);

        Throwable cause = actualChangeStreamException.getCause();
        assertSame(exception, cause);
        Throwable[] suppressed = actualChangeStreamException.getSuppressed();
        assertEquals(0, suppressed.length);
        assertEquals("Msg", actualChangeStreamException.getLocalizedMessage());
        assertEquals("Msg", actualChangeStreamException.getMessage());
        assertNull(cause.getLocalizedMessage());
        assertNull(cause.getCause());
        assertNull(cause.getMessage());
        assertSame(suppressed, cause.getSuppressed());
    }
}
