/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

/**
 * Tracks the delay which Spanner connector waits for
 * the next Change Stream Event
 */
public class DelayChangeStreamEventsMetricEvent implements MetricEvent {

    private final int delayChangeStreamEvents;

    public DelayChangeStreamEventsMetricEvent(int delayChangeStreamEvents) {
        this.delayChangeStreamEvents = delayChangeStreamEvents;
    }

    public int getDelayChangeStreamEvents() {
        return delayChangeStreamEvents;
    }
}
