/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.StreamEventMetadata;

/**
 * Common contract for all types of the Spanner Change Stream events
 */
public interface ChangeStreamEvent {
    Timestamp getRecordTimestamp();

    StreamEventMetadata getMetadata();
}
