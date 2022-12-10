/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

/**
 * Tracks number of partitions
 */
public class ChildPartitionsMetricEvent implements MetricEvent {
    private final int numberPartitions;

    public ChildPartitionsMetricEvent(int numberPartitions) {
        this.numberPartitions = numberPartitions;
    }

    public int getNumberPartitions() {
        return numberPartitions;
    }
}
