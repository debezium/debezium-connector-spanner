/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.state;

import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;

/**
 * Notifies that status of the partition has been changed
 */
public class PartitionStatusUpdateEvent implements TaskStateChangeEvent {
    private final String token;
    private final PartitionStateEnum state;

    public PartitionStatusUpdateEvent(String token, PartitionStateEnum state) {
        this.token = token;
        this.state = state;
    }

    public String getToken() {
        return token;
    }

    public PartitionStateEnum getState() {
        return state;
    }
}
