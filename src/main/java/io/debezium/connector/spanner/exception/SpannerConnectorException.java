/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.exception;

/**
 * Runtime common spanner exception
 */
public class SpannerConnectorException extends RuntimeException {
    public SpannerConnectorException(String msg) {
        super(msg);
    }

    public SpannerConnectorException(String msg, Throwable ex) {
        super(msg, ex);
    }
}
