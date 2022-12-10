/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.context.source;

import java.time.Instant;

import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.connector.spanner.SpannerConnectorConfig;

/**
 * Information provided by Spanner connector in source field or offsets
 */
public class SourceInfo extends BaseSourceInfo {

    public static final String SOURCE_KEY = "source";

    public static final String PROJECT_ID_KEY = "project_id";

    public static final String INSTANCE_ID_KEY = "instance_id";

    public static final String DATABASE_ID_KEY = "database_id";

    public static final String CHANGE_STREAM_NAME_KEY = "change_stream_name";

    public static final String TABLE_KEY = "table";

    public static final String READ_AT_TIMESTAMP_KEY = "read_at_timestamp";
    public static final String SERVER_TRANSACTIONAL_ID_KEY = "server_transaction_id";

    public static final String NUMBER_OF_RECORDS_IN_TRANSACTION = "number_records_in_transaction";

    public static final String TRANSACTION_TAG = "transaction_tag";

    public static final String SYSTEM_TRANSACTION = "system_transaction";

    public static final String LOW_WATERMARK_KEY = "low_watermark";

    public static final String VALUE_CAPTURE_TYPE = "value_capture_type";

    public static final String PARTITION_TOKEN = "partition_token";

    public static final String MOD_NUMBER = "mod_number";

    public static final String NUMBER_PARTITIONS_IN_TRANSACTION = "number_of_partitions_in_transaction";

    public static final String LAST_RECORD_IN_TRANSACTION_IN_PARTITION = "is_last_record_in_transaction_in_partition";

    private final String projectId;
    private final String instanceId;
    private final String databaseId;
    private final String changeStreamName;

    private final String tableName;
    private final Instant recordTimestamp;

    private final Instant commitTimestamp;

    private final String serverTransactionId;

    private final Long recordSequence;

    private final Instant lowWatermark;

    private final Instant readAtTimestamp;

    private final Long numberRecordsInTransaction;

    private final String transactionTag;

    private final Boolean isSystemTransaction;

    private final String valueCaptureType;

    private final String partitionToken;

    private final Integer modNumber;

    private final Boolean isLastRecordInTransactionInPartition;

    private final Long numberOfPartitionsInTransaction;

    public SourceInfo(SpannerConnectorConfig connectorConfig, String tableName, Instant recordTimestamp,
                      Instant commitTimestamp, Instant readAtTimestamp, String serverTransactionId,
                      Long recordSequence, Instant lowWatermark, Long numberRecordsInTransaction,
                      String transactionTag, Boolean isSystemTransaction, String valueCaptureType, String partitionToken,
                      Integer modNumber, Boolean isLastRecordInTransactionInPartition, Long numberOfPartitionsInTransaction) {
        super(connectorConfig);
        this.projectId = connectorConfig.projectId();
        this.instanceId = connectorConfig.instanceId();
        this.databaseId = connectorConfig.databaseId();
        this.changeStreamName = connectorConfig.changeStreamName();
        this.tableName = tableName;
        this.recordTimestamp = recordTimestamp;
        this.commitTimestamp = commitTimestamp;
        this.serverTransactionId = serverTransactionId;
        this.recordSequence = recordSequence;
        this.lowWatermark = lowWatermark;
        this.readAtTimestamp = readAtTimestamp;
        this.numberRecordsInTransaction = numberRecordsInTransaction;
        this.transactionTag = transactionTag;
        this.isSystemTransaction = isSystemTransaction;
        this.valueCaptureType = valueCaptureType;
        this.partitionToken = partitionToken;
        this.modNumber = modNumber;
        this.isLastRecordInTransactionInPartition = isLastRecordInTransactionInPartition;
        this.numberOfPartitionsInTransaction = numberOfPartitionsInTransaction;
    }

    @Override
    public Instant timestamp() {
        return recordTimestamp;
    }

    @Override
    public String database() {
        return this.databaseId;
    }

    @Override
    public String sequence() {
        return recordSequence == null ? null : recordSequence.toString();
    }

    public Instant getRecordTimestamp() {
        return recordTimestamp;
    }

    public Instant getCommitTimestamp() {
        return commitTimestamp;
    }

    public String getServerTransactionId() {
        return serverTransactionId;
    }

    public Long getRecordSequence() {
        return recordSequence;
    }

    public String getTableName() {
        return tableName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public String getChangeStreamName() {
        return changeStreamName;
    }

    public String getProjectId() {
        return projectId;
    }

    public Instant getLowWatermark() {
        return lowWatermark;
    }

    public Instant getReadAtTimestamp() {
        return readAtTimestamp;
    }

    public Long getNumberRecordsInTransaction() {
        return numberRecordsInTransaction;
    }

    public String getTransactionTag() {
        return transactionTag;
    }

    public Boolean isSystemTransaction() {
        return isSystemTransaction;
    }

    public String getValueCaptureType() {
        return valueCaptureType;
    }

    public String getPartitionToken() {
        return partitionToken;
    }

    public Integer getModNumber() {
        return modNumber;
    }

    public Boolean getLastRecordInTransactionInPartition() {
        return isLastRecordInTransactionInPartition;
    }

    public Long getNumberOfPartitionsInTransaction() {
        return numberOfPartitionsInTransaction;
    }

}
