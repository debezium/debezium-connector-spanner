/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.state;

import java.util.List;

import io.debezium.connector.spanner.db.model.Partition;

/**
 * Notifies that new partition has been created
 */
public class NewPartitionsEvent implements TaskStateChangeEvent {
    private final List<Partition> partitions;

    public NewPartitionsEvent(List<Partition> partitions) {
        this.partitions = partitions;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }
}
