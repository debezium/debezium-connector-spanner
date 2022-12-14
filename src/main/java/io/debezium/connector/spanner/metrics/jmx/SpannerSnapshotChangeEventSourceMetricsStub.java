/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.jmx;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.data.Envelope;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Metrics related to the snapshot phase of the Spanner connector.
 *
 * Not implemented yet.
 */
public class SpannerSnapshotChangeEventSourceMetricsStub implements SnapshotChangeEventSourceMetrics<SpannerPartition> {

    @Override
    public void register() {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void unregister() {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void onEvent(SpannerPartition partition, DataCollectionId source, OffsetContext offset, Object key, Struct value, Envelope.Operation operation) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void onFilteredEvent(SpannerPartition partition, String event) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void onFilteredEvent(SpannerPartition partition, String event, Envelope.Operation operation) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void onErroneousEvent(SpannerPartition partition, String event) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void onErroneousEvent(SpannerPartition partition, String event, Envelope.Operation operation) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void onConnectorEvent(SpannerPartition partition, ConnectorEvent event) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void snapshotStarted(SpannerPartition partition) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void snapshotPaused(SpannerPartition partition) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void snapshotResumed(SpannerPartition partition) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void monitoredDataCollectionsDetermined(SpannerPartition partition, Iterable<? extends DataCollectionId> dataCollectionIds) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void snapshotCompleted(SpannerPartition partition) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void snapshotAborted(SpannerPartition partition) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void dataCollectionSnapshotCompleted(SpannerPartition partition, DataCollectionId dataCollectionId, long numRows) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void rowsScanned(SpannerPartition partition, TableId tableId, long numRows) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void currentChunk(SpannerPartition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo) {
        // spanner connector doesn't support snapshots
    }

    @Override
    public void currentChunk(SpannerPartition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo) {
        // spanner connector doesn't support snapshots
    }
}
