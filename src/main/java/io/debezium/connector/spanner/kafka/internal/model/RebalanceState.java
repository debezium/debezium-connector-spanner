/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

public enum RebalanceState {
    START_INITIAL_SYNC, // State when the task is first initialized, and ready to start building
                        // the internal TaskSyncEvent state incrementally.
    INITIAL_INCREMENTED_STATE_COMPLETED, // State when the task has finished building the internal
                                         // TaskSyncEvent state and is ready to connect to the
                                         // rebalance topic.
    NEW_EPOCH_STARTED, // State when the task has received a NEW_EPOCH message and is ready to
                       // start sharing change stream partitions.
    ZOMBIE_STATE // State when the task is not part of the new epoch and should stop sending messages.
}
