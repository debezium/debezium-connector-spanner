/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;

/**
 * The {@code ChangeStreamEventConsumer} is callback from {@link ChangeStream} class, supplies spanner event
 */
@FunctionalInterface
public interface ChangeStreamEventConsumer {
    void acceptChangeStreamEvent(ChangeStreamEvent changeStreamEvent) throws InterruptedException;
}
