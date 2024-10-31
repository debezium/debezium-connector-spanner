/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.context.offset;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.spanner.context.source.SourceInfo;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Implementation of {@code OffsetContext}.
 * Updates offset from {@code DataChangeEvent}, {@code HeartbeatEvent}, {@code ChildPartitionsEvent} events
 */
public class SpannerOffsetContext implements OffsetContext {

    private final SourceInfo sourceInfo;
    private final TransactionContext transactionContext;

    private final PartitionOffset partitionOffset;

    public SpannerOffsetContext(SourceInfo sourceInfo, PartitionOffset partitionOffset, TransactionContext transactionContext) {
        this.sourceInfo = sourceInfo;
        this.partitionOffset = partitionOffset;
        this.transactionContext = transactionContext;
    }

    public SpannerOffsetContext(PartitionOffset partitionOffset, TransactionContext transactionContext) {
        this.sourceInfo = null;
        this.partitionOffset = partitionOffset;
        this.transactionContext = transactionContext;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isInitialSnapshotRunning() {
        return false;
    }

    @Override
    public void markSnapshotRecord(SnapshotRecord record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void preSnapshotStart(boolean onDemand) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void preSnapshotCompletion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void postSnapshotCompletion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void event(DataCollectionId dataCollectionId, Instant instant) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void incrementalSnapshotEvents() {
        OffsetContext.super.incrementalSnapshotEvents();
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return OffsetContext.super.getIncrementalSnapshotContext();
    }

    @Override
    public Map<String, ?> getOffset() {
        return partitionOffset.getOffset();
    }
}
