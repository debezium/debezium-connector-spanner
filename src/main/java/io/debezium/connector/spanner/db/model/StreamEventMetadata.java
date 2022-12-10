/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.cloud.Timestamp;

/**
 * Contains all information about the spanner event
 */
public class StreamEventMetadata {

    private String partitionToken;

    private Timestamp recordTimestamp;

    private Timestamp partitionStartTimestamp;

    private Timestamp partitionEndTimestamp;

    private Timestamp queryStartedAt;

    private Timestamp recordStreamStartedAt;

    private Timestamp recordStreamEndedAt;

    private Timestamp recordReadAt;

    private long totalStreamTimeMillis;
    private long numberOfRecordsRead;

    private StreamEventMetadata() {
    }

    private StreamEventMetadata(
                                String partitionToken,
                                Timestamp recordTimestamp,
                                Timestamp partitionStartTimestamp,
                                Timestamp partitionEndTimestamp,
                                Timestamp queryStartedAt,
                                Timestamp recordStreamStartedAt,
                                Timestamp recordStreamEndedAt,
                                Timestamp recordReadAt,
                                long totalStreamTimeMillis,
                                long numberOfRecordsRead) {
        this.partitionToken = partitionToken;
        this.recordTimestamp = recordTimestamp;

        this.partitionStartTimestamp = partitionStartTimestamp;
        this.partitionEndTimestamp = partitionEndTimestamp;

        this.queryStartedAt = queryStartedAt;
        this.recordStreamStartedAt = recordStreamStartedAt;
        this.recordStreamEndedAt = recordStreamEndedAt;
        this.recordReadAt = recordReadAt;
        this.totalStreamTimeMillis = totalStreamTimeMillis;
        this.numberOfRecordsRead = numberOfRecordsRead;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * The partition token that produced this change stream record.
     */
    public String getPartitionToken() {
        return partitionToken;
    }

    /**
     * The Cloud Spanner timestamp time when this record occurred.
     */
    public Timestamp getRecordTimestamp() {
        return recordTimestamp;
    }

    /**
     * The start time for the partition change stream query, which produced this record.
     */
    public Timestamp getPartitionStartTimestamp() {
        return partitionStartTimestamp;
    }

    /**
     * The end time for the partition change stream query, which produced this record.
     */
    public Timestamp getPartitionEndTimestamp() {
        return partitionEndTimestamp;
    }

    /**
     * The time that the change stream query which produced this record started.
     */
    public Timestamp getQueryStartedAt() {
        return queryStartedAt;
    }

    /**
     * The time at which the record started to be streamed.
     */
    public Timestamp getRecordStreamStartedAt() {
        return recordStreamStartedAt;
    }

    /**
     * The time at which the record finished streaming.
     */
    public Timestamp getRecordStreamEndedAt() {
        return recordStreamEndedAt;
    }

    /**
     * The time at which the record was fully read.
     */
    public Timestamp getRecordReadAt() {
        return recordReadAt;
    }

    /**
     * The total streaming time (in millis) for this record.
     */
    public long getTotalStreamTimeMillis() {
        return totalStreamTimeMillis;
    }

    /**
     * The number of records read in the partition change stream query before reading this record.
     */
    public long getNumberOfRecordsRead() {
        return numberOfRecordsRead;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StreamEventMetadata)) {
            return false;
        }
        StreamEventMetadata metadata = (StreamEventMetadata) o;
        return totalStreamTimeMillis == metadata.totalStreamTimeMillis
                && numberOfRecordsRead == metadata.numberOfRecordsRead
                && Objects.equals(partitionToken, metadata.partitionToken)
                && Objects.equals(recordTimestamp, metadata.recordTimestamp)
                && Objects.equals(partitionStartTimestamp, metadata.partitionStartTimestamp)
                && Objects.equals(partitionEndTimestamp, metadata.partitionEndTimestamp)
                && Objects.equals(queryStartedAt, metadata.queryStartedAt)
                && Objects.equals(recordStreamStartedAt, metadata.recordStreamStartedAt)
                && Objects.equals(recordStreamEndedAt, metadata.recordStreamEndedAt)
                && Objects.equals(recordReadAt, metadata.recordReadAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                partitionToken,
                recordTimestamp,
                partitionStartTimestamp,
                partitionEndTimestamp,
                queryStartedAt,
                recordStreamStartedAt,
                recordStreamEndedAt,
                recordReadAt,
                totalStreamTimeMillis,
                numberOfRecordsRead);
    }

    @Override
    public String toString() {
        return "ChangeStreamRecordMetadata{"
                + "partitionToken='"
                + partitionToken
                + '\''
                + ", recordTimestamp="
                + recordTimestamp
                + ", partitionStartTimestamp="
                + partitionStartTimestamp
                + ", partitionEndTimestamp="
                + partitionEndTimestamp
                + ", queryStartedAt="
                + queryStartedAt
                + ", recordStreamStartedAt="
                + recordStreamStartedAt
                + ", recordStreamEndedAt="
                + recordStreamEndedAt
                + ", recordReadAt="
                + recordReadAt
                + ", totalStreamTimeMillis="
                + totalStreamTimeMillis
                + ", numberOfRecordsRead="
                + numberOfRecordsRead
                + '}';
    }

    public static class Builder {
        private String partitionToken;
        private Timestamp recordTimestamp;
        private Timestamp partitionStartTimestamp;
        private Timestamp partitionEndTimestamp;
        private Timestamp queryStartedAt;
        private Timestamp recordStreamStartedAt;
        private Timestamp recordStreamEndedAt;
        private Timestamp recordReadAt;
        private long totalStreamTimeMillis;
        private long numberOfRecordsRead;

        /**
         * Sets the partition token where this record originated from.
         *
         * @param partitionToken the partition token to be set
         * @return Builder
         */
        public Builder withPartitionToken(String partitionToken) {
            this.partitionToken = partitionToken;
            return this;
        }

        /**
         * Sets the timestamp of when this record occurred.
         *
         * @param recordTimestamp the timestamp to be set
         * @return Builder
         */
        public Builder withRecordTimestamp(Timestamp recordTimestamp) {
            this.recordTimestamp = recordTimestamp;
            return this;
        }

        /**
         * Sets the start time for the partition change stream query that originated this record.
         *
         * @param partitionStartTimestamp the timestamp to be set
         * @return Builder
         */
        public Builder withPartitionStartTimestamp(Timestamp partitionStartTimestamp) {
            this.partitionStartTimestamp = partitionStartTimestamp;
            return this;
        }

        /**
         * Sets the end time for the partition change stream query that originated this record.
         *
         * @param partitionEndTimestamp the timestamp to be set
         * @return Builder
         */
        public Builder withPartitionEndTimestamp(Timestamp partitionEndTimestamp) {
            this.partitionEndTimestamp = partitionEndTimestamp;
            return this;
        }

        /**
         * Sets the time that the change stream query which produced this record started.
         *
         * @param queryStartedAt the timestamp to be set
         * @return Builder
         */
        public Builder withQueryStartedAt(Timestamp queryStartedAt) {
            this.queryStartedAt = queryStartedAt;
            return this;
        }

        /**
         * Sets the time at which the record started to be streamed.
         *
         * @param recordStreamStartedAt the timestamp to be set
         * @return Builder
         */
        public Builder withRecordStreamStartedAt(Timestamp recordStreamStartedAt) {
            this.recordStreamStartedAt = recordStreamStartedAt;
            return this;
        }

        /**
         * Sets the time at which the record finished streaming.
         *
         * @param recordStreamEndedAt the timestamp to be set
         * @return Builder
         */
        public Builder withRecordStreamEndedAt(Timestamp recordStreamEndedAt) {
            this.recordStreamEndedAt = recordStreamEndedAt;
            return this;
        }

        /**
         * Sets the time at which the record was fully read.
         *
         * @param recordReadAt the timestamp to be set
         * @return Builder
         */
        public Builder withRecordReadAt(Timestamp recordReadAt) {
            this.recordReadAt = recordReadAt;
            return this;
        }

        /**
         * Sets the total streaming time (in millis) for this record.
         *
         * @param totalStreamTimeMillis the total time in millis
         * @return Builder
         */
        public Builder withTotalStreamTimeMillis(long totalStreamTimeMillis) {
            this.totalStreamTimeMillis = totalStreamTimeMillis;
            return this;
        }

        /**
         * Sets the number of records read in the partition change stream query before reading this
         * record.
         *
         * @param numberOfRecordsRead the number of records read
         * @return Builder
         */
        public Builder withNumberOfRecordsRead(long numberOfRecordsRead) {
            this.numberOfRecordsRead = numberOfRecordsRead;
            return this;
        }

        /**
         * Builds the {@link StreamEventMetadata}.
         *
         * @return ChangeStreamRecordMetadata
         */
        public StreamEventMetadata build() {
            return new StreamEventMetadata(
                    partitionToken,
                    recordTimestamp,
                    partitionStartTimestamp,
                    partitionEndTimestamp,
                    queryStartedAt,
                    recordStreamStartedAt,
                    recordStreamEndedAt,
                    recordReadAt,
                    totalStreamTimeMillis,
                    numberOfRecordsRead);
        }
    }
}
