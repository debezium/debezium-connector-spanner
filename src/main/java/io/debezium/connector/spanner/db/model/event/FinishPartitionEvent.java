/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.StreamEventMetadata;

/**
 * Specific DTO for Spanner Finish partition event
 */
public class FinishPartitionEvent implements ChangeStreamEvent {

    private final StreamEventMetadata metadata;

    public FinishPartitionEvent(Partition partition) {
        this.metadata = StreamEventMetadata.newBuilder()
                .withPartitionToken(partition.getToken())
                .withPartitionEndTimestamp(partition.getEndTimestamp())
                .withPartitionStartTimestamp(partition.getStartTimestamp())
                .build();
    }

    @Override
    public Timestamp getRecordTimestamp() {
        return null;
    }

    @Override
    public StreamEventMetadata getMetadata() {
        return metadata;
    }
}
