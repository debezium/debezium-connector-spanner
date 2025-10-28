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
 * Specific DTO for Spanner Change Stream partition event event
 */
public class PartitionEventEvent implements ChangeStreamEvent {

    private final Timestamp commitTimestamp;

    private final String recordSequence;
    private final String partitionToken;
    private final List<String> sourcePartitions;
    private final List<String> destinationPartitions;
    private final StreamEventMetadata metadata;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionEventEvent that = (PartitionEventEvent) o;

        if (!Objects.equals(commitTimestamp, that.commitTimestamp)) {
            return false;
        }
        if (!Objects.equals(recordSequence, that.recordSequence)) {
            return false;
        }
        if (!Objects.equals(partitionToken, that.partitionToken)) {
            return false;
        }
        if (!Objects.equals(sourcePartitions, that.sourcePartitions)) {
            return false;
        }
        if (!Objects.equals(destinationPartitions, that.destinationPartitions)) {
            return false;
        }
        return Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        int result = commitTimestamp != null ? commitTimestamp.hashCode() : 0;
        result = 31 * result + (recordSequence != null ? recordSequence.hashCode() : 0);
        result = 31 * result + (partitionToken != null ? partitionToken.hashCode() : 0);
        result = 31 * result + (sourcePartitions != null ? sourcePartitions.hashCode() : 0);
        result = 31 * result + (destinationPartitions != null ? destinationPartitions.hashCode() : 0);
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }

    /**
     * Constructs a partition event record containing key range moves among
     * partitions.
     *
     * @param commitTimestamp       the timestamp which this record was committed in
     *                              Cloud Spanner
     * @param recordSequence        the order within a partition and a transaction
     *                              in which the record was put to the stream
     * @param partitionToken        the unique identifier of the partition that
     *                              generated this record.
     * @param sourcePartitions      list of source partitions from which the key
     *                              ranges are moved
     * @param destinationPartitions list of destination partitions to which the key
     *                              ranges are moved
     * @param metadata              connector execution metadata for the given
     *                              record
     */
    public PartitionEventEvent(Timestamp commitTimestamp,
                               String recordSequence,
                               String partitionToken,
                               List<String> sourcePartitions,
                               List<String> destinationPartitions,
                               StreamEventMetadata metadata) {
        this.commitTimestamp = commitTimestamp;
        this.recordSequence = recordSequence;
        this.partitionToken = partitionToken;
        this.sourcePartitions = sourcePartitions;
        this.destinationPartitions = destinationPartitions;
        this.metadata = metadata;
    }

    /**
     * The timestamp at which the key range moves were committed in Cloud Spanner.
     *
     * @return the commit timestamp
     */
    @Override
    public Timestamp getRecordTimestamp() {
        return getCommitTimestamp();
    }

    /**
     * The timestamp at which the key range moves were committed in Cloud Spanner.
     *
     * @return the commit timestamp
     */
    public Timestamp getCommitTimestamp() {
        return commitTimestamp;
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

    /**
     * List of source partitions from which the key ranges are moved.
     *
     * @return source partitions
     */
    public List<String> getSourcePartitions() {
        return sourcePartitions;
    }

    /**
     * List of destination partitions to which the key ranges are moved.
     *
     * @return destination partitions
     */
    public List<String> getDestinationPartitions() {
        return destinationPartitions;
    }

    @Override
    public StreamEventMetadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "PartitionEventEvent{"
                + "commitTimestamp="
                + commitTimestamp
                + ", recordSequence='"
                + recordSequence
                + '\''
                + ", partitionToken="
                + partitionToken
                + ", sourcePartitions="
                + sourcePartitions
                + ", destinationPartitions="
                + destinationPartitions
                + ", metadata="
                + metadata
                + '}';
    }
}