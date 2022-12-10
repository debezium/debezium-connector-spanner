/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream.exception;

/**
 * Super class for change stream exceptions
 */
public class ChangeStreamException extends Exception {
    public ChangeStreamException(String msg, Exception ex) {
        super(msg, ex);
    }

    public ChangeStreamException(Exception ex) {
        super(ex);
    }

    public ChangeStreamException(String msg) {
        super(msg);
    }
}
