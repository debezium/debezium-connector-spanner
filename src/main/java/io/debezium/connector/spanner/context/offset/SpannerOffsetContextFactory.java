/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.context.offset;

import io.debezium.connector.spanner.context.source.SourceInfo;
import io.debezium.connector.spanner.context.source.SourceInfoFactory;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.event.FinishPartitionEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;
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

    public SpannerOffsetContext getNullOffsetContext(FinishPartitionEvent finishPartitionEvent) {
        PartitionOffset partitionOffset = new PartitionOffset(finishPartitionEvent.getMetadata());
        return new SpannerOffsetContext(partitionOffset, transactionContext);
    }
}
