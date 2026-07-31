/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.state;

import java.util.List;

import com.google.cloud.Timestamp;

/**
 * Notifies that a mutable key range partition processed a MoveIn event.
 * Carries the commit timestamp, the record sequence of the MoveIn event, and the
 * source partition tokens so the destination partition can be gated until the
 * source partitions have processed their corresponding MoveOut events.
 */
public class MoveInNotificationEvent implements TaskStateChangeEvent {
    private final String token;
    private final Timestamp commitTimestamp;
    private final String recordSequence;
    private final List<String> sourcePartitionTokens;

    public MoveInNotificationEvent(String token, Timestamp commitTimestamp, String recordSequence, List<String> sourcePartitionTokens) {
        this.token = token;
        this.commitTimestamp = commitTimestamp;
        this.recordSequence = recordSequence;
        this.sourcePartitionTokens = sourcePartitionTokens;
    }

    public String getToken() {
        return token;
    }

    public Timestamp getCommitTimestamp() {
        return commitTimestamp;
    }

    public String getRecordSequence() {
        return recordSequence;
    }

    public List<String> getSourcePartitionTokens() {
        return sourcePartitionTokens;
    }
}
