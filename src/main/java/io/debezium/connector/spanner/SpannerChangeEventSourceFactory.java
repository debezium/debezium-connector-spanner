/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.connector.spanner.context.offset.SpannerOffsetContextFactory;
import io.debezium.connector.spanner.context.source.SourceInfoFactory;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.metrics.SpannerMeter;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;

/** Creates SpannerStreamingChangeEventSource and SnapshotChangeEventSource */
public class SpannerChangeEventSourceFactory
        implements ChangeEventSourceFactory<SpannerPartition, SpannerOffsetContext> {

    private final SpannerConnectorConfig connectorConfig;
    private final SpannerEventDispatcher dispatcher;

    private final ErrorHandler errorHandler;
    private final SchemaRegistry schemaRegistry;

    private final SpannerMeter spannerMeter;

    private final ChangeStream changeStream;

    private final SourceInfoFactory sourceInfoFactory;

    private final PartitionManager partitionManager;

    public SpannerChangeEventSourceFactory(
                                           SpannerConnectorConfig connectorConfig,
                                           SpannerEventDispatcher dispatcher,
                                           ErrorHandler errorHandler,
                                           SchemaRegistry schemaRegistry,
                                           SpannerMeter spannerMeter,
                                           ChangeStream changeStream,
                                           SourceInfoFactory sourceInfoFactory,
                                           PartitionManager partitionManager) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.schemaRegistry = schemaRegistry;
        this.spannerMeter = spannerMeter;
        this.changeStream = changeStream;
        this.sourceInfoFactory = sourceInfoFactory;
        this.partitionManager = partitionManager;
    }

    @Override
    public SnapshotChangeEventSource<SpannerPartition, SpannerOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener snapshotProgressListener) {
        return (context, partition, previousOffset) -> SnapshotResult.skipped(null);
    }

    @Override
    public SpannerStreamingChangeEventSource getStreamingChangeEventSource() {

        StreamEventQueue streamEventQueue = new StreamEventQueue(
                connectorConfig.queueCapacity(), spannerMeter.getMetricsEventPublisher());

        SpannerOffsetContextFactory offsetContextFactory = new SpannerOffsetContextFactory(sourceInfoFactory);

        return new SpannerStreamingChangeEventSource(
                errorHandler,
                changeStream,
                streamEventQueue,
                spannerMeter.getMetricsEventPublisher(),
                partitionManager,
                schemaRegistry,
                dispatcher,
                connectorConfig.isFinishingPartitionAfterCommit(),
                offsetContextFactory);
    }
}
