/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

/**
 * Tracks total and remaining capacity of the Spanner Queue
 */
public class SpannerEventQueueUpdateEvent implements MetricEvent {

    private final int totalCapacity;

    private final int remainingCapacity;

    public SpannerEventQueueUpdateEvent(int totalCapacity, int remainingCapacity) {
        this.totalCapacity = totalCapacity;
        this.remainingCapacity = remainingCapacity;
    }

    public int getTotalCapacity() {
        return totalCapacity;
    }

    public int getRemainingCapacity() {
        return remainingCapacity;
    }
}
