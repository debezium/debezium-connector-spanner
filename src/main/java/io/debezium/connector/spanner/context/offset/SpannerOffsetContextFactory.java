/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.context.offset;

import io.debezium.connector.spanner.context.source.SourceInfo;
import io.debezium.connector.spanner.context.source.SourceInfoFactory;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEventEvent;
import io.debezium.pipeline.txmetadata.TransactionContext;

public class SpannerOffsetContextFactory {
    private final SourceInfoFactory sourceInfoFactory;

    private final TransactionContext transactionContext;

    public SpannerOffsetContextFactory(SourceInfoFactory sourceInfoFactory) {
        this.sourceInfoFactory = sourceInfoFactory;
        this.transactionContext = new TransactionContext();
    }

    public SpannerOffsetContext getOffsetContextFromDataChangeEvent(int modNumber, DataChangeEvent dataChangeEvent) throws InterruptedException {
        SourceInfo sourceInfo = sourceInfoFactory.getSourceInfo(modNumber, dataChangeEvent);
        PartitionOffset partitionOffset = new PartitionOffset(dataChangeEvent.getCommitTimestamp(), dataChangeEvent.getMetadata());
        return new SpannerOffsetContext(sourceInfo, partitionOffset, transactionContext);
    }

    public SpannerOffsetContext getOffsetContextFromHeartbeatEvent(HeartbeatEvent heartbeatEvent) {
        PartitionOffset partitionOffset = new PartitionOffset(heartbeatEvent.getRecordTimestamp(), heartbeatEvent.getMetadata());
        return new SpannerOffsetContext(partitionOffset, transactionContext);
    }

    /**
     * Builds an offset context from a MoveOut {@link PartitionEventEvent}'s own commit timestamp.
     *
     * <p>A source partition under heavy MoveOut churn (repeatedly splitting off pieces of its key
     * range) can emit thousands of these events per window without a single {@code DataChangeEvent}
     * or {@code HeartbeatEvent} in between. Since only those two event types previously advanced the
     * Kafka Connect-committed offset (via {@link #getOffsetContextFromDataChangeEvent} /
     * {@link #getOffsetContextFromHeartbeatEvent}), the partition's committed offset — and therefore
     * the low watermark, which falls back to it — stayed frozen at the start of the window despite the
     * partition being extremely active, only catching up once the outer window boundary closed. Using
     * the MoveOut event's own commit timestamp here lets real progress on this partition advance the
     * offset immediately, the same way a heartbeat would.
     */
    public SpannerOffsetContext getOffsetContextFromPartitionEventEvent(PartitionEventEvent partitionEventEvent) {
        PartitionOffset partitionOffset = new PartitionOffset(partitionEventEvent.getRecordTimestamp(), partitionEventEvent.getMetadata());
        return new SpannerOffsetContext(partitionOffset, transactionContext);
    }
}
