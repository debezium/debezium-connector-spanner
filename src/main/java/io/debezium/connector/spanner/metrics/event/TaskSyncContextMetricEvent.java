/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Exposes internal state of the task
 */
public class TaskSyncContextMetricEvent implements MetricEvent {
    private final TaskSyncContext taskSyncContext;

    public TaskSyncContextMetricEvent(TaskSyncContext taskSyncContext) {
        this.taskSyncContext = taskSyncContext;
    }

    public TaskSyncContext getTaskSyncContext() {
        return taskSyncContext;
    }
}
