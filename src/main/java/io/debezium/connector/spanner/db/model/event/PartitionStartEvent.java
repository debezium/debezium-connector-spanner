/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

import java.util.List;
import java.util.Objects;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.StreamEventMetadata;

/**
 * Specific DTO for Spanner Change Stream partition start event
 */
public class PartitionStartEvent implements ChangeStreamEvent {

    private final Timestamp startTimestamp;

    private final String recordSequence;
    private final List<String> partitionTokens;
    private final boolean isFromInitialPartition;
    private final StreamEventMetadata metadata;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionStartEvent that = (PartitionStartEvent) o;

        if (!Objects.equals(startTimestamp, that.startTimestamp)) {
            return false;
        }
        if (!Objects.equals(recordSequence, that.recordSequence)) {
            return false;
        }
        if (!Objects.equals(partitionTokens, that.partitionTokens)) {
            return false;
        }
        if (!Objects.equals(isFromInitialPartition, that.isFromInitialPartition)) {
            return false;
        }
        return Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        int result = startTimestamp != null ? startTimestamp.hashCode() : 0;
        result = 31 * result + (recordSequence != null ? recordSequence.hashCode() : 0);
        result = 31 * result + (partitionTokens != null ? partitionTokens.hashCode() : 0);
        result = 31 * result + (isFromInitialPartition ? 1 : 0);
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }

    /**
     * Constructs a partition start record containing one or more partition tokens.
     *
     * @param startTimestamp         the timestamp when this partition started in
     *                               Cloud Spanner
     * @param recordSequence         the order within a partition in which the
     *                               record was put to the stream
     * @param partitionTokens        partition tokens emitted within this record
     * @param isFromInitialPartition indicates if the PartitionStartRecord comes the
     *                               initial query
     * @param metadata               connector execution metadata for the given
     *                               record
     */
    public PartitionStartEvent(Timestamp startTimestamp,
                               String recordSequence,
                               List<String> partitionTokens,
                               boolean isFromInitialPartition,
                               StreamEventMetadata metadata) {
        this.startTimestamp = startTimestamp;
        this.recordSequence = recordSequence;
        this.partitionTokens = partitionTokens;
        this.isFromInitialPartition = isFromInitialPartition;
        this.metadata = metadata;
    }

    /**
     * Returns the start timestamp in the PartitionStartRecord. The caller must
     * use this time as the change stream query start timestamp for the new
     * partitions.
     *
     * @return the start timestamp of the partition
     */
    @Override
    public Timestamp getRecordTimestamp() {
        return getStartTimestamp();
    }

    /**
     * It is the partition_start_time of the partitions in the PartitionStartRecord.
     * This partition_start_time is guaranteed to be the same across all the
     * partitions yielded from a PartitionStartRecord. When users start new queries
     * with the partition tokens in the PartitionStartRecord, the returned records
     * must have a timestamp >= partition_start_time.
     *
     * @return the start timestamp of the partition
     */
    public Timestamp getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Indicates the order in which a record was put to the stream. Is unique and
     * increasing within a partition. It is relative to the scope of partition,
     * commit timestamp. It is useful for readers downstream to deduplicate any
     * records that were read/recorded.
     *
     * @return record sequence of the record
     */
    public String getRecordSequence() {
        return recordSequence;
    }

    /**
     * List of partitions yielded within this record.
     *
     * @return partition tokens
     */
    public List<String> getPartitionTokens() {
        return partitionTokens;
    }

    /*
     * Indicates if the PartitionStartRecord comes from the initial query.
     *
     * @return true if from initial partition, false otherwise
     */
    public boolean isFromInitialPartition() {
        return isFromInitialPartition;
    }

    @Override
    public StreamEventMetadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "PartitionStartEvent{"
                + "startTimestamp="
                + startTimestamp
                + ", recordSequence='"
                + recordSequence
                + '\''
                + ", partitionToken="
                + partitionTokens
                + ", isFromInitialPartition="
                + isFromInitialPartition
                + ", metadata="
                + metadata
                + '}';
    }
}
