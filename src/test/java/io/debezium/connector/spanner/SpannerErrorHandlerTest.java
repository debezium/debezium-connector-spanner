/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SpannerErrorHandlerTest {

    @Test
    void testConstructor() {
        SpannerErrorHandler actualSpannerErrorHandler = new SpannerErrorHandler(null, null);
        assertTrue(actualSpannerErrorHandler.isRetriable(null));
    }
}
