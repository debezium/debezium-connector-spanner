/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

/**
 * Tracks number of active partitions per each task
 */
public class ActiveQueriesUpdateMetricEvent implements MetricEvent {

    private final int activeQueries;

    public ActiveQueriesUpdateMetricEvent(int activeQueries) {
        this.activeQueries = activeQueries;
    }

    public int getActiveQueries() {
        return activeQueries;
    }
}
