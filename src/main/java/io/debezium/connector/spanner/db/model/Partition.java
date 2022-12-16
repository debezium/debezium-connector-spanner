/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import java.util.Set;

import com.google.cloud.Timestamp;
import com.google.common.base.Preconditions;

/**
 * A partition represents a Spanner partition.
 */
public class Partition {

    private final String partitionToken;
    private final Set<String> parentTokens;
    private final Timestamp startTimestamp;
    private final Timestamp endTimestamp;

    private final String originPartitionToken;

    public Partition(String partitionToken, Set<String> parentTokens, Timestamp startTimestamp,
                     Timestamp endTimestamp, String originPartitionToken) {
        this.partitionToken = partitionToken;
        this.parentTokens = parentTokens;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.originPartitionToken = originPartitionToken;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getToken() {
        return partitionToken;
    }

    public Set<String> getParentTokens() {
        return parentTokens;
    }

    public Timestamp getStartTimestamp() {
        return startTimestamp;
    }

    public Timestamp getEndTimestamp() {
        return endTimestamp;
    }

    public String getOriginPartitionToken() {
        return originPartitionToken;
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    @Override
    public String toString() {
        return "Partition{" +
                "partitionToken='" + partitionToken + '\'' +
                ", parentTokens=" + parentTokens +
                ", startTimestamp=" + startTimestamp +
                ", endTimestamp=" + endTimestamp +
                ", originPartitionToken='" + originPartitionToken + '\'' +
                '}';
    }

    public static class Builder {

        private String partitionToken;
        private Set<String> parentTokens;
        private Timestamp startTimestamp;
        private Timestamp endTimestamp;

        private String originPartitionToken;

        public Builder() {
        }

        private Builder(Partition partition) {
            this.partitionToken = partition.partitionToken;
            this.startTimestamp = partition.startTimestamp;
            this.endTimestamp = partition.endTimestamp;
            this.originPartitionToken = partition.originPartitionToken;
        }

        public Builder token(String partitionToken) {
            this.partitionToken = partitionToken;
            return this;
        }

        public Builder parentTokens(Set<String> parentTokens) {
            this.parentTokens = parentTokens;
            return this;
        }

        public Builder startTimestamp(Timestamp startTimestamp) {
            this.startTimestamp = startTimestamp;
            return this;
        }

        public Builder endTimestamp(Timestamp endTimestamp) {
            this.endTimestamp = endTimestamp;
            return this;
        }

        public Builder originPartitionToken(String originPartitionToken) {
            this.originPartitionToken = originPartitionToken;
            return this;
        }

        public Partition build() {
            Preconditions.checkState(partitionToken != null, "partitionToken");
            Preconditions.checkState(parentTokens != null, "parentTokens");
            Preconditions.checkState(startTimestamp != null, "startTimestamp");

            return new Partition(
                    partitionToken,
                    parentTokens,
                    startTimestamp,
                    endTimestamp,
                    originPartitionToken);
        }
    }
}
