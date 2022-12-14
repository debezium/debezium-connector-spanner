/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream.exception;

import com.google.cloud.spanner.SpannerException;

import io.debezium.connector.spanner.db.model.Partition;

/**
 * The change stream fail exception. Reaction of {@code ErrorCode.OUT_OF_RANGE} error from Spanner
 */
public class OutOfRangeChangeStreamException extends FailureChangeStreamException {
    private final transient Partition partition;

    public OutOfRangeChangeStreamException(Partition partition, SpannerException ex) {
        super("Change stream start time or partition offset are incorrect, " +
                "please reset offsets - remove sync topic " +
                "or set `gcp.spanner.start.time` according to change stream retention period. " +
                "Partition: " + partition.toString(), ex);
        this.partition = partition;
    }

    public Partition getPartition() {
        return partition;
    }
}
