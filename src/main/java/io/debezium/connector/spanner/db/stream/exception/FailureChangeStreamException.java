/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream.exception;

/**
 * The change stream fail exception. Stream execution stops
 */
public class FailureChangeStreamException extends ChangeStreamException {

    public FailureChangeStreamException(String msg, Exception ex) {
        super(msg, ex);
    }

    public FailureChangeStreamException(Exception ex) {
        super(ex);
    }

    public FailureChangeStreamException(String msg) {
        super(msg);
    }
}
