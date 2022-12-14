/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.scaler;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * Utility to calculate metrics required for
 * task auto-scaling, based on internal states
 * of current tasks
 */
public class TaskScalerUtil {
    private TaskScalerUtil() {
    }

    public static long partitionsInWorkCount(TaskSyncEvent taskSyncEvent) {
        if (taskSyncEvent == null) {
            return 0;
        }

        return taskSyncEvent
                .getTaskStates()
                .values()
                .stream()
                .flatMap(t -> t.getPartitions().stream())
                .filter(p -> inProgressPartitionState(p.getState()))
                .map(PartitionState::getToken)
                .distinct()
                .count();
    }

    public static int tasksCount(TaskSyncEvent taskSyncEvent) {
        if (taskSyncEvent == null) {
            return 0;
        }

        return taskSyncEvent
                .getTaskStates()
                .size();
    }

    public static long idlingTaskCount(TaskSyncEvent taskSyncEvent) {
        if (taskSyncEvent == null) {
            return 0;
        }

        return taskSyncEvent
                .getTaskStates()
                .values()
                .stream()
                .filter(TaskScalerUtil::isIdlingTask)
                .count();
    }

    private static boolean isIdlingTask(TaskState task) {
        boolean hasInProgressPartition = task.getPartitions()
                .stream()
                .anyMatch(p -> inProgressPartitionState(p.getState()));

        return !hasInProgressPartition;
    }

    private static boolean inProgressPartitionState(PartitionStateEnum state) {
        return state != PartitionStateEnum.FINISHED && state != PartitionStateEnum.REMOVED;
    }
}
