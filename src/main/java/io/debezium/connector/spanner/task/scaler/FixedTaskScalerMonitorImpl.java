/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.scaler;

/**
 * A stub version of the scaler monitor
 */
public class FixedTaskScalerMonitorImpl implements TaskScalerMonitor {
    private final int maxTasks;

    public FixedTaskScalerMonitorImpl(int maxTasks) {
        this.maxTasks = maxTasks;
    }

    @Override
    public int start() {
        return maxTasks;
    }

    @Override
    public int getRequiredTasksCount() {
        return maxTasks;
    }

    @Override
    public void shutdown() {
    }
}
