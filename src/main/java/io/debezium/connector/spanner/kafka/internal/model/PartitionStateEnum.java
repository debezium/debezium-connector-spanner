/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

public enum PartitionStateEnum {
    // The partition has been discovered.
    CREATED,
    // The partition is ready for streaming, as all parents are finished.
    READY_FOR_STREAMING,
    // The partition has been scheduled to be streamed.
    SCHEDULED,
    // The partition has started being streamed.
    RUNNING,
    // The partition has finished being streamed.
    FINISHED,
    // The partition should be cleared from both the internal offsets and internal task state.
    REMOVED
}
