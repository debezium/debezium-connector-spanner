/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream.exception;

/**
 * The change stream fail exception. A queried partition has been stuck.
 */
public class StuckPartitionException extends FailureChangeStreamException {
    public StuckPartitionException(String token) {
        super("Spanner partition querying has been stuck: " + token);
    }
}
