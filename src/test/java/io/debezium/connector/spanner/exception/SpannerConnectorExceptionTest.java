/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class SpannerConnectorExceptionTest {

    @Test
    void testConstructor() {
        SpannerConnectorException actualSpannerConnectorException = new SpannerConnectorException("Msg");
        assertEquals("Msg", actualSpannerConnectorException.getMessage());
    }

    @Test
    void testConstructorThrowable() {
        Throwable throwable = new Throwable();
        SpannerConnectorException actualSpannerConnectorException = new SpannerConnectorException("Msg", throwable);

        Throwable cause = actualSpannerConnectorException.getCause();
        assertSame(throwable, cause);
        assertEquals("Msg", actualSpannerConnectorException.getMessage());
        assertNull(cause.getMessage());
    }
}
