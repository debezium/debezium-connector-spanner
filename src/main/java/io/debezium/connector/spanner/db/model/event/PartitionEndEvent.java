/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

import java.util.Objects;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.StreamEventMetadata;

/**
 * Specific DTO for Spanner Change Stream partition end event
 */
public class PartitionEndEvent implements ChangeStreamEvent {

    private final Timestamp endTimestamp;

    private final String recordSequence;
    private final String partitionToken;
    private final StreamEventMetadata metadata;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionEndEvent that = (PartitionEndEvent) o;

        if (!Objects.equals(endTimestamp, that.endTimestamp)) {
            return false;
        }
        if (!Objects.equals(recordSequence, that.recordSequence)) {
            return false;
        }
        if (!Objects.equals(partitionToken, that.partitionToken)) {
            return false;
        }
        return Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        int result = endTimestamp != null ? endTimestamp.hashCode() : 0;
        result = 31 * result + (recordSequence != null ? recordSequence.hashCode() : 0);
        result = 31 * result + (partitionToken != null ? partitionToken.hashCode() : 0);
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }

    /**
     * Constructs a partition end record.
     *
     * @param endTimestamp   the timestamp when this partition ends in Cloud Spanner
     * @param recordSequence the order within a partition and a transaction
     *                       in which the record was put to the stream
     * @param partitionToken the unique identifier of the partition that generated
     *                       this record.
     * @param metadata       connector execution metadata for the given
     *                       record
     */
    public PartitionEndEvent(Timestamp endTimestamp,
                             String recordSequence, String partitionToken,
                             StreamEventMetadata metadata) {
        this.endTimestamp = endTimestamp;
        this.recordSequence = recordSequence;
        this.partitionToken = partitionToken;
        this.metadata = metadata;
    }

    /**
     * Returns the end timestamp in the PartitionEndRecord.
     *
     * @return the end timestamp of the partition
     */
    @Override
    public Timestamp getRecordTimestamp() {
        return getEndTimestamp();
    }

    /**
     * It is the partition_end_time of the partitions in the PartitionEndRecord.
     * No further records are expected to be retrieved on this partition.
     *
     * @return the end timestamp of the partition
     */
    public Timestamp getEndTimestamp() {
        return endTimestamp;
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
     * The unique identifier of the partition that generated this record.
     *
     * @return the partition token
     */
    public String getPartitionToken() {
        return partitionToken;
    }

    @Override
    public StreamEventMetadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "PartitionEndEvent{"
                + "endTimestamp="
                + endTimestamp
                + ", recordSequence='"
                + recordSequence
                + '\''
                + ", partitionToken="
                + partitionToken
                + ", metadata="
                + metadata
                + '}';
    }
}
