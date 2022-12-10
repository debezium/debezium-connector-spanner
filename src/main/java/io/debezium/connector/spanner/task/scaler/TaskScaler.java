/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.scaler;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * Checks if the current tasks count is okay for
 * the current load or needs to be scaled-out/in
 */
public class TaskScaler {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskScaler.class);
    private final SpannerConnectorConfig connectorConfig;
    private final ConnectorContext connectorContext;

    public TaskScaler(SpannerConnectorConfig connectorConfig, ConnectorContext connectorContext) {
        this.connectorConfig = connectorConfig;
        this.connectorContext = connectorContext;
    }

    public int ensureTasksScale(TaskSyncEvent taskSyncEvent) {
        int actualTasksCount = TaskScalerUtil.tasksCount(taskSyncEvent);
        return ensureTasksScale(taskSyncEvent, actualTasksCount);
    }

    public int ensureTasksScale(TaskSyncEvent taskSyncEvent, int actualTasksCount) {
        int requiredTasksCount = getTasksCount(taskSyncEvent, actualTasksCount);
        long epochOffset = taskSyncEvent.getEpochOffset();
        if (requiredTasksCount == actualTasksCount) {
            LOGGER.info("ensureTasksScale: no scaling is required, tasks count: {}, epochOffset: {}", actualTasksCount, epochOffset);
            return actualTasksCount;
        }
        if (requiredTasksCount > actualTasksCount) {
            LOGGER.info("ensureTasksScale: needs to scale out, tasks count: actual = {}, required = {}, epochOffset: {}", actualTasksCount, requiredTasksCount,
                    epochOffset);
        }
        else {
            LOGGER.info("ensureTasksScale: needs to scale in, tasks count: actual = {}, required = {}, epochOffset: {}", actualTasksCount, requiredTasksCount,
                    epochOffset);
        }
        connectorContext.requestTaskReconfiguration();
        return requiredTasksCount;
    }

    public int getTasksCount(TaskSyncEvent taskSyncEvent, int currentTasksCount) {
        int desiredPartitionsTasks = connectorConfig.getDesiredPartitionsTasks();
        int maxTasks = connectorConfig.getMaxTasks();
        int minTasks = connectorConfig.getMinTasks();
        long partitionsInWorkCount = TaskScalerUtil.partitionsInWorkCount(taskSyncEvent);
        long idlingTaskCount = TaskScalerUtil.idlingTaskCount(taskSyncEvent);

        LOGGER.info("getTasksCount: currentTasksCount = {}, " +
                "desiredPartitionsTasks = {}, " +
                "maxTasks = {}, " +
                "minTasks = {}, " +
                "partitionsInWorkCount = {}, " +
                "idlingTaskCount = {}",
                currentTasksCount, desiredPartitionsTasks, maxTasks, minTasks, partitionsInWorkCount, idlingTaskCount);

        return TaskScaleCalculator.newTasksCount(
                currentTasksCount,
                desiredPartitionsTasks,
                maxTasks,
                minTasks,
                partitionsInWorkCount,
                idlingTaskCount);
    }
}
