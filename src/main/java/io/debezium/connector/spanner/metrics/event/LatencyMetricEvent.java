/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

/**
 * Tracks different latencies during the record lifecycle,
 * starting from the commit in the database and finishing after
 * receiving the confirmation that record has been committed
 * to Kafka.
 */
public class LatencyMetricEvent implements MetricEvent {

    private final Long totalLatency;

    private final Long readToEmitLatency;

    private final Long spannerLatency;

    private final Long commitToEmitLatency;

    private final Long commitToPublishLatency;

    private final Long emitToPublishLatency;

    private final Long lowWatermarkLag;
    private final Long ownConnectorLatency;

    public LatencyMetricEvent(Long totalLatency, Long readToEmitLatency, Long spannerLatency, Long commitToEmitLatency,
                              Long commitToPublishLatency, Long emitToPublishLatency, Long lowWatermarkLag,
                              Long ownConnectorLatency) {
        this.totalLatency = totalLatency;
        this.readToEmitLatency = readToEmitLatency;
        this.spannerLatency = spannerLatency;
        this.commitToEmitLatency = commitToEmitLatency;
        this.commitToPublishLatency = commitToPublishLatency;
        this.emitToPublishLatency = emitToPublishLatency;
        this.lowWatermarkLag = lowWatermarkLag;
        this.ownConnectorLatency = ownConnectorLatency;
    }

    public Long getTotalLatency() {
        return totalLatency;
    }

    public Long getReadToEmitLatency() {
        return readToEmitLatency;
    }

    public Long getSpannerLatency() {
        return spannerLatency;
    }

    public Long getCommitToEmitLatency() {
        return commitToEmitLatency;
    }

    public Long getCommitToPublishLatency() {
        return commitToPublishLatency;
    }

    public Long getEmitToPublishLatency() {
        return emitToPublishLatency;
    }

    public Long getOwnConnectorLatency() {
        return ownConnectorLatency;
    }

    public Long getLowWatermarkLag() {
        return lowWatermarkLag;
    }

}
