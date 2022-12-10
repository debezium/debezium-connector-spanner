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

class FailureChangeStreamExceptionTest {

    @Test
    void testExceptionConstructor() {
        Exception exception = new Exception();
        FailureChangeStreamException actualFailureChangeStreamException = new FailureChangeStreamException(exception);
        Throwable cause = actualFailureChangeStreamException.getCause();
        assertSame(exception, cause);
        Throwable[] suppressed = actualFailureChangeStreamException.getSuppressed();
        assertEquals(0, suppressed.length);
        assertEquals("java.lang.Exception", actualFailureChangeStreamException.getLocalizedMessage());
        assertEquals("java.lang.Exception", actualFailureChangeStreamException.getMessage());
        assertNull(cause.getLocalizedMessage());
        assertNull(cause.getCause());
        assertNull(cause.getMessage());
        assertSame(suppressed, cause.getSuppressed());
    }

    @Test
    void testStringConstructor() {
        FailureChangeStreamException actualFailureChangeStreamException = new FailureChangeStreamException("Msg");
        assertNull(actualFailureChangeStreamException.getCause());
        assertEquals(0, actualFailureChangeStreamException.getSuppressed().length);
        assertEquals("Msg", actualFailureChangeStreamException.getMessage());
        assertEquals("Msg", actualFailureChangeStreamException.getLocalizedMessage());
    }

    @Test
    void testExceptionStringConstructor() {
        Exception exception = new Exception();
        FailureChangeStreamException actualFailureChangeStreamException = new FailureChangeStreamException("Msg", exception);

        Throwable cause = actualFailureChangeStreamException.getCause();
        assertSame(exception, cause);
        Throwable[] suppressed = actualFailureChangeStreamException.getSuppressed();
        assertEquals(0, suppressed.length);
        assertEquals("Msg", actualFailureChangeStreamException.getLocalizedMessage());
        assertEquals("Msg", actualFailureChangeStreamException.getMessage());
        assertNull(cause.getLocalizedMessage());
        assertNull(cause.getCause());
        assertNull(cause.getMessage());
        assertSame(suppressed, cause.getSuppressed());
    }
}
