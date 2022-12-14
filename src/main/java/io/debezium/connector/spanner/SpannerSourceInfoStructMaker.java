/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.connector.spanner.context.source.SourceInfo;

/**
 * Builds Struct from the SourceInfo
 */
public class SpannerSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public SpannerSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("com.google.spanner.connector.Source")
                .field(SourceInfo.PROJECT_ID_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.INSTANCE_ID_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.DATABASE_ID_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.CHANGE_STREAM_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.SERVER_TRANSACTIONAL_ID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.LOW_WATERMARK_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.READ_AT_TIMESTAMP_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.NUMBER_OF_RECORDS_IN_TRANSACTION, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.TRANSACTION_TAG, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.SYSTEM_TRANSACTION, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field(SourceInfo.VALUE_CAPTURE_TYPE, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.PARTITION_TOKEN, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.MOD_NUMBER, Schema.OPTIONAL_INT32_SCHEMA)
                .field(SourceInfo.LAST_RECORD_IN_TRANSACTION_IN_PARTITION, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field(SourceInfo.NUMBER_PARTITIONS_IN_TRANSACTION, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        if (sourceInfo.getDatabaseId() == null || sourceInfo.database() == null) {
            throw new IllegalStateException("databaseId is null");
        }
        if (sourceInfo.getTableName() == null) {
            throw new IllegalStateException("table name is null");
        }

        if (sourceInfo.getInstanceId() == null) {
            throw new IllegalStateException("instanceId is null");
        }

        if (sourceInfo.getProjectId() == null) {
            throw new IllegalStateException("projectId is null");
        }

        return buildStruct(sourceInfo);
    }

    private Struct buildStruct(SourceInfo sourceInfo) {
        Struct result = super.commonStruct(sourceInfo);
        result.put(SourceInfo.PROJECT_ID_KEY, sourceInfo.getProjectId());
        result.put(SourceInfo.INSTANCE_ID_KEY, sourceInfo.getInstanceId());
        result.put(SourceInfo.DATABASE_ID_KEY, sourceInfo.getDatabaseId());
        result.put(SourceInfo.TABLE_KEY, sourceInfo.getTableName());
        result.put(SourceInfo.CHANGE_STREAM_NAME_KEY, sourceInfo.getChangeStreamName());

        if (sourceInfo.getReadAtTimestamp() != null) {
            result.put(SourceInfo.READ_AT_TIMESTAMP_KEY, sourceInfo.getReadAtTimestamp().toEpochMilli());
        }

        if (sourceInfo.getLowWatermark() != null) {
            result.put(SourceInfo.LOW_WATERMARK_KEY, sourceInfo.getLowWatermark().toEpochMilli());
        }

        if (sourceInfo.getServerTransactionId() != null) {
            result.put(SourceInfo.SERVER_TRANSACTIONAL_ID_KEY, sourceInfo.getServerTransactionId());
        }

        if (sourceInfo.getNumberRecordsInTransaction() != null) {
            result.put(SourceInfo.NUMBER_OF_RECORDS_IN_TRANSACTION, sourceInfo.getNumberRecordsInTransaction());
        }

        if (sourceInfo.getTransactionTag() != null) {
            result.put(SourceInfo.TRANSACTION_TAG, sourceInfo.getTransactionTag());
        }

        if (sourceInfo.isSystemTransaction() != null) {
            result.put(SourceInfo.SYSTEM_TRANSACTION, sourceInfo.isSystemTransaction());
        }

        if (sourceInfo.getValueCaptureType() != null) {
            result.put(SourceInfo.VALUE_CAPTURE_TYPE, sourceInfo.getValueCaptureType());
        }

        if (sourceInfo.getPartitionToken() != null) {
            result.put(SourceInfo.PARTITION_TOKEN, sourceInfo.getPartitionToken());
        }

        if (sourceInfo.getModNumber() != null) {
            result.put(SourceInfo.MOD_NUMBER, sourceInfo.getModNumber());
        }

        if (sourceInfo.getLastRecordInTransactionInPartition() != null) {
            result.put(SourceInfo.LAST_RECORD_IN_TRANSACTION_IN_PARTITION, sourceInfo.getLastRecordInTransactionInPartition());
        }

        if (sourceInfo.getNumberOfPartitionsInTransaction() != null) {
            result.put(SourceInfo.NUMBER_PARTITIONS_IN_TRANSACTION, sourceInfo.getNumberOfPartitionsInTransaction());
        }
        return result;
    }
}
