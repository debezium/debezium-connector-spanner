/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.context.source;

import java.time.Instant;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.context.offset.LowWatermarkProvider;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;

/**
 * Creates {@link SourceInfo} from the input {@link DataChangeEvent}
 */
public class SourceInfoFactory {
    private final SpannerConnectorConfig connectorConfig;
    private final LowWatermarkProvider lowWatermarkProvider;

    public SourceInfoFactory(SpannerConnectorConfig connectorConfig, LowWatermarkProvider lowWatermarkProvider) {
        this.connectorConfig = connectorConfig;
        this.lowWatermarkProvider = lowWatermarkProvider;
    }

    public SourceInfo getSourceInfo(int modNumber, DataChangeEvent dataChangeEvent) throws InterruptedException {
        Instant commitTimestamp = dataChangeEvent.getCommitTimestamp().toSqlTimestamp().toInstant();
        Instant recordTimestamp = dataChangeEvent.getRecordTimestamp().toSqlTimestamp().toInstant();
        Instant readAtTimestamp = dataChangeEvent.getMetadata().getRecordReadAt().toSqlTimestamp().toInstant();
        String serverTransactionId = dataChangeEvent.getServerTransactionId();
        Long recordSequence = Long.parseLong(dataChangeEvent.getRecordSequence());
        Long numberRecordInTransaction = dataChangeEvent.getNumberOfRecordsInTransaction();
        String transactionTag = dataChangeEvent.getTransactionTag();
        boolean systemTransaction = dataChangeEvent.isSystemTransaction();
        String valueCaptureType = dataChangeEvent.getValueCaptureType().toString();
        String partitionToken = dataChangeEvent.getPartitionToken();
        boolean isLastRecordInTransactionInPartition = dataChangeEvent.isLastRecordInTransactionInPartition();
        long numberOfPartitionsInTransaction = dataChangeEvent.getNumberOfPartitionsInTransaction();

        Instant lowWatermark = null;

        if (connectorConfig.isLowWatermarkEnabled()) {
            lowWatermark = lowWatermarkProvider.getLowWatermark().toSqlTimestamp().toInstant();
        }

        return new SourceInfo(connectorConfig, dataChangeEvent.getTableName(), recordTimestamp, commitTimestamp,
                readAtTimestamp, serverTransactionId, recordSequence, lowWatermark, numberRecordInTransaction,
                transactionTag, systemTransaction, valueCaptureType, partitionToken, modNumber,
                isLastRecordInTransactionInPartition, numberOfPartitionsInTransaction);
    }

    public SourceInfo getSourceInfoForLowWatermarkStamp(TableId tableId) throws InterruptedException {

        Instant lowWatermark = null;

        if (connectorConfig.isLowWatermarkEnabled()) {
            lowWatermark = lowWatermarkProvider.getLowWatermark().toSqlTimestamp().toInstant();
        }

        return new SourceInfo(connectorConfig, tableId.getTableName(), null, null,
                null, null, null, lowWatermark, null,
                null, null, null, null, null,
                null, null);
    }
}
