/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

public class TaskStateChangeQueueUpdateMetricEvent implements MetricEvent {

    private final int remainingCapacity;

    public TaskStateChangeQueueUpdateMetricEvent(int remainingCapacity) {
        this.remainingCapacity = remainingCapacity;
    }

    public int getRemainingCapacity() {
        return remainingCapacity;
    }

}
