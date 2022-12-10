/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

import java.time.Instant;

/**
 * Tracks the time duration between requesting and receiving
 * offsets from Kafka Connect.
 */
public class OffsetReceivingTimeMetricEvent implements MetricEvent {
    private final long time;

    private OffsetReceivingTimeMetricEvent(long time) {
        this.time = time;
    }

    public static OffsetReceivingTimeMetricEvent from(Instant startTime) {
        return new OffsetReceivingTimeMetricEvent(Instant.now().toEpochMilli() - startTime.toEpochMilli());
    }

    public long getTime() {
        return time;
    }
}
