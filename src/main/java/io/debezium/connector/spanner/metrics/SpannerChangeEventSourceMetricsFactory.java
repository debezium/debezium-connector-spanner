/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.connector.spanner.context.source.SpannerSourceTaskContext;
import io.debezium.connector.spanner.metrics.jmx.SpannerSnapshotChangeEventSourceMetricsStub;
import io.debezium.connector.spanner.metrics.jmx.SpannerStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Creates {@link SnapshotChangeEventSourceMetrics} and {@link StreamingChangeEventSourceMetrics}
 */
public class SpannerChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory<SpannerPartition> {

    private final SpannerMeter spannerMeter;

    public SpannerChangeEventSourceMetricsFactory(SpannerMeter spannerMeter) {
        this.spannerMeter = spannerMeter;
    }

    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<SpannerPartition> getSnapshotMetrics(T taskContext,
                                                                                                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                  EventMetadataProvider eventMetadataProvider) {
        return new SpannerSnapshotChangeEventSourceMetricsStub();
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<SpannerPartition> getStreamingMetrics(T taskContext,
                                                                                                                    ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                    EventMetadataProvider eventMetadataProvider) {
        return new SpannerStreamingChangeEventSourceMetrics((SpannerSourceTaskContext) taskContext,
                changeEventQueueMetrics, eventMetadataProvider, spannerMeter);
    }
}
