/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.leader.rebalancer;

import java.util.Map;

import io.debezium.connector.spanner.kafka.internal.model.TaskState;

/**
 * Rebalancing partitions across tasks
 */
public interface TaskPartitionRebalancer {
    TaskState rebalance(TaskState leaderTaskState,
                        Map<String, TaskState> survivedTasks,
                        Map<String, TaskState> obsoleteTaskStates);
}
