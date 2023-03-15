/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

import java.util.List;
import java.util.Objects;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.Mod;
import io.debezium.connector.spanner.db.model.ModType;
import io.debezium.connector.spanner.db.model.StreamEventMetadata;
import io.debezium.connector.spanner.db.model.ValueCaptureType;
import io.debezium.connector.spanner.db.model.schema.Column;

/**
 * Specific DTO for Spanner Change Stream Data-change event
 */
public class DataChangeEvent implements ChangeStreamEvent {

    private final String partitionToken;

    @Override
    public boolean equals(@javax.annotation.Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataChangeEvent)) {
            return false;
        }
        DataChangeEvent that = (DataChangeEvent) o;
        return isLastRecordInTransactionInPartition == that.isLastRecordInTransactionInPartition
                && numberOfRecordsInTransaction == that.numberOfRecordsInTransaction
                && numberOfPartitionsInTransaction == that.numberOfPartitionsInTransaction
                && Objects.equals(transactionTag, that.transactionTag)
                && isSystemTransaction == that.isSystemTransaction
                && Objects.equals(partitionToken, that.partitionToken)
                && Objects.equals(commitTimestamp, that.commitTimestamp)
                && Objects.equals(serverTransactionId, that.serverTransactionId)
                && Objects.equals(recordSequence, that.recordSequence)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(rowType, that.rowType)
                && Objects.equals(mods, that.mods)
                && modType == that.modType
                && valueCaptureType == that.valueCaptureType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                partitionToken,
                commitTimestamp,
                serverTransactionId,
                isLastRecordInTransactionInPartition,
                recordSequence,
                tableName,
                rowType,
                mods,
                modType,
                valueCaptureType,
                numberOfRecordsInTransaction,
                numberOfPartitionsInTransaction,
                transactionTag,
                isSystemTransaction);
    }

    private final Timestamp commitTimestamp;

    private final String serverTransactionId;
    private final boolean isLastRecordInTransactionInPartition;
    private final String recordSequence;
    private final String tableName;
    private final List<Column> rowType;
    private final List<Mod> mods;
    private final ModType modType;
    private final ValueCaptureType valueCaptureType;
    private final long numberOfRecordsInTransaction;
    private final long numberOfPartitionsInTransaction;

    private final String transactionTag;

    private final boolean isSystemTransaction;
    private final StreamEventMetadata metadata;

    /**
     * Constructs a data change record for a given partition, at a given timestamp, for a given
     * transaction. The data change record needs to be given information about the table modified, the
     * type of primary keys and modified columns, the modifications themselves and other metadata.
     *
     * @param partitionToken                       the unique identifier of the partition that generated this record
     * @param commitTimestamp                      the timestamp at which the modifications within were committed in Cloud
     *                                             Spanner
     * @param serverTransactionId                  the unique transaction id in which the modifications occurred
     * @param isLastRecordInTransactionInPartition indicates whether this record is the last emitted
     *                                             for the given transaction in the given partition
     * @param recordSequence                       indicates the order in which this record was put into the change stream
     *                                             in the scope of a partition, commit timestamp and transaction tuple
     * @param tableName                            the name of the table in which the modifications occurred
     * @param rowType                              the type of the primary keys and modified columns
     * @param mods                                 the modifications occurred
     * @param modType                              the operation that caused the modification to occur
     * @param valueCaptureType                     the capture type of the change stream
     * @param numberOfRecordsInTransaction         the total number of records for the given transaction
     * @param numberOfPartitionsInTransaction      the total number of partitions within the given
     *                                             transaction
     * @param metadata                             connector execution metadata for the given record
     */
    public DataChangeEvent(
                           String partitionToken,
                           Timestamp commitTimestamp,
                           String serverTransactionId,
                           boolean isLastRecordInTransactionInPartition,
                           String recordSequence,
                           String tableName,
                           List<Column> rowType,
                           List<Mod> mods,
                           ModType modType,
                           ValueCaptureType valueCaptureType,
                           long numberOfRecordsInTransaction,
                           long numberOfPartitionsInTransaction,
                           String transactionTag,
                           boolean isSystemTransaction,
                           StreamEventMetadata metadata) {
        this.commitTimestamp = commitTimestamp;
        this.partitionToken = partitionToken;
        this.serverTransactionId = serverTransactionId;
        this.isLastRecordInTransactionInPartition = isLastRecordInTransactionInPartition;
        this.recordSequence = recordSequence;
        this.tableName = tableName;
        this.rowType = rowType;
        this.mods = mods;
        this.modType = modType;
        this.valueCaptureType = valueCaptureType;
        this.numberOfRecordsInTransaction = numberOfRecordsInTransaction;
        this.numberOfPartitionsInTransaction = numberOfPartitionsInTransaction;
        this.transactionTag = transactionTag;
        this.isSystemTransaction = isSystemTransaction;
        this.metadata = metadata;
    }

    /**
     * The timestamp at which the modifications within were committed in Cloud Spanner.
     */
    @Override
    public Timestamp getRecordTimestamp() {
        return commitTimestamp;
    }

    /**
     * The unique identifier of the partition that generated this record.
     */
    public String getPartitionToken() {
        return partitionToken;
    }

    /**
     * The timestamp at which the modifications within were committed in Cloud Spanner.
     */
    public Timestamp getCommitTimestamp() {
        return commitTimestamp;
    }

    /**
     * The unique transaction id in which the modifications occurred.
     */
    public String getServerTransactionId() {
        return serverTransactionId;
    }

    /**
     * Indicates whether this record is the last emitted for the given transaction in the given
     * partition.
     */
    public boolean isLastRecordInTransactionInPartition() {
        return isLastRecordInTransactionInPartition;
    }

    /**
     * Indicates the order in which this record was put into the change stream in the scope of a
     * partition, commit timestamp and transaction tuple.
     */
    public String getRecordSequence() {
        return recordSequence;
    }

    /**
     * The name of the table in which the modifications within this record occurred.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * The type of the primary keys and modified columns within this record.
     */
    public List<Column> getRowType() {
        return rowType;
    }

    /**
     * The modifications within this record.
     */
    public List<Mod> getMods() {
        return mods;
    }

    /**
     * The type of operation that caused the modifications within this record.
     */
    public ModType getModType() {
        return modType;
    }

    /**
     * The capture type of the change stream that generated this record.
     */
    public ValueCaptureType getValueCaptureType() {
        return valueCaptureType;
    }

    /**
     * The total number of data change records for the given transaction.
     */
    public long getNumberOfRecordsInTransaction() {
        return numberOfRecordsInTransaction;
    }

    /**
     * The total number of partitions for the given transaction.
     */
    public long getNumberOfPartitionsInTransaction() {
        return numberOfPartitionsInTransaction;
    }

    /**
     * Transaction tag associated with this transaction.
     */
    public String getTransactionTag() {
        return transactionTag;
    }

    /**
     * Indicates whether the transaction is a system transaction.
     */
    public boolean isSystemTransaction() {
        return isSystemTransaction;
    }

    /**
     * The connector execution metadata for this record.
     */
    @Override
    public StreamEventMetadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "DataChangeEvent{" +
                "partitionToken='" + partitionToken + '\'' +
                ", commitTimestamp=" + commitTimestamp +
                ", serverTransactionId='" + serverTransactionId + '\'' +
                ", isLastRecordInTransactionInPartition=" + isLastRecordInTransactionInPartition +
                ", recordSequence='" + recordSequence + '\'' +
                ", tableName='" + tableName + '\'' +
                ", rowType=" + rowType +
                ", mods=" + mods +
                ", modType=" + modType +
                ", valueCaptureType=" + valueCaptureType +
                ", numberOfRecordsInTransaction=" + numberOfRecordsInTransaction +
                ", numberOfPartitionsInTransaction=" + numberOfPartitionsInTransaction +
                ", transactionTag='" + transactionTag + '\'' +
                ", isSystemTransaction=" + isSystemTransaction +
                ", metadata=" + metadata +
                '}';
    }
}
