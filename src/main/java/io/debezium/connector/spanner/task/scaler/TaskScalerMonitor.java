/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.scaler;

/**
 * Monitors tasks states to decide when tasks
 * should be scaled-out/in
 */
public interface TaskScalerMonitor {
    int start() throws InterruptedException;

    int getRequiredTasksCount();

    void shutdown();
}
