/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

/**
 * Tracks the number of intervals waiting but not receiving Heartbeat
 * records from the Spanner Change Stream
 */
public class StuckHeartbeatIntervalsMetricEvent implements MetricEvent {

    private final int stuckHeartbeatIntervals;

    public StuckHeartbeatIntervalsMetricEvent(int stuckHeartbeatIntervals) {
        this.stuckHeartbeatIntervals = stuckHeartbeatIntervals;
    }

    public int getStuckHeartbeatIntervals() {
        return stuckHeartbeatIntervals;
    }
}
