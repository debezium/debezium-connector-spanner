/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.scaler;

/**
 * Calculates a new number of tasks which should be
 * present in the connector after the scaling
 */
public class TaskScaleCalculator {

    private static final double SCALE_OUT_TASKS_STEP = 0.2;
    private static final double SCALE_IN_TASKS_THRESHOLD = 0.5;

    private TaskScaleCalculator() {
    }

    public static int newTasksCount(int currentTasksCount,
                                    int desiredPartitionsTasks,
                                    int tasksMax,
                                    int tasksMin,
                                    long partitionsInWorkCount,
                                    long idlingTaskCount) {

        int newTasksCount = Math.max(currentTasksCount, tasksMin);
        newTasksCount = Math.min(newTasksCount, tasksMax);

        int addTaskCount = 0;
        while (partitionTaskRatio(newTasksCount, partitionsInWorkCount) > desiredPartitionsTasks && newTasksCount < tasksMax) {
            addTaskCount = Math.min((int) Math.ceil(newTasksCount * SCALE_OUT_TASKS_STEP), tasksMax - newTasksCount);
            newTasksCount += addTaskCount;
        }

        if (newTasksCount > tasksMin
                && partitionTaskRatio(newTasksCount, partitionsInWorkCount) < desiredPartitionsTasks
                && idlingTaskCount > newTasksCount * SCALE_IN_TASKS_THRESHOLD) {
            newTasksCount = Math.max((int) (newTasksCount * SCALE_IN_TASKS_THRESHOLD), tasksMin);
        }

        return newTasksCount;
    }

    private static double partitionTaskRatio(long tasksCount, long partitionsInWorkCount) {
        return tasksCount == 0 ? 0 : (double) partitionsInWorkCount / tasksCount;
    }
}
