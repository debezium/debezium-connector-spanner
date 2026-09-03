/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.state;

import com.google.cloud.Timestamp;

/**
 * Event fired at the end of each mutable-stream sliding window to persist the
 * window boundary timestamp and boundary record sequence in
 * {@link io.debezium.connector.spanner.kafka.internal.model.PartitionState}.
 */
public class WindowAdvancedEvent implements TaskStateChangeEvent {

    private final String token;
    private final Timestamp processedTimestamp;
    private final String lastBoundaryRecordSequence;

    public WindowAdvancedEvent(String token, Timestamp processedTimestamp, String lastBoundaryRecordSequence) {
        this.token = token;
        this.processedTimestamp = processedTimestamp;
        this.lastBoundaryRecordSequence = lastBoundaryRecordSequence;
    }

    public String getToken() {
        return token;
    }

    public Timestamp getProcessedTimestamp() {
        return processedTimestamp;
    }

    public String getLastBoundaryRecordSequence() {
        return lastBoundaryRecordSequence;
    }
}
