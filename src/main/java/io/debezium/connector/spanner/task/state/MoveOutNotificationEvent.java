/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.state;

import java.util.List;

import com.google.cloud.Timestamp;

/**
 * Notifies that a mutable key range partition processed a MoveOut event.
 * Carries the commit timestamp and destination partition tokens for future ordering support.
 */
public class MoveOutNotificationEvent implements TaskStateChangeEvent {
    private final String token;
    private final Timestamp commitTimestamp;
    private final List<String> destinationTokens;

    public MoveOutNotificationEvent(String token, Timestamp commitTimestamp, List<String> destinationTokens) {
        this.token = token;
        this.commitTimestamp = commitTimestamp;
        this.destinationTokens = destinationTokens;
    }

    public String getToken() {
        return token;
    }

    public Timestamp getCommitTimestamp() {
        return commitTimestamp;
    }

    public List<String> getDestinationTokens() {
        return destinationTokens;
    }
}
