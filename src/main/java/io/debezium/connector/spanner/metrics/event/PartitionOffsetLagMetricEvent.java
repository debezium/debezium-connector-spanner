/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

import java.time.Instant;

import com.google.cloud.Timestamp;

/**
 * Tracks time difference between now and the start of the partition
 */
public class PartitionOffsetLagMetricEvent implements MetricEvent {

    private final String token;
    private final Long offsetLag;

    private PartitionOffsetLagMetricEvent(String token, Long offsetLag) {
        this.token = token;
        this.offsetLag = offsetLag;
    }

    public String getToken() {
        return token;
    }

    public Long getOffsetLag() {
        return offsetLag;
    }

    public static PartitionOffsetLagMetricEvent from(String token, Timestamp timestamp) {
        return new PartitionOffsetLagMetricEvent(token, Instant.now().toEpochMilli()
                - timestamp.toSqlTimestamp().toInstant().toEpochMilli());
    }

}
