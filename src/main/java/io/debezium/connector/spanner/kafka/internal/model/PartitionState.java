/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

import java.util.Objects;
import java.util.Set;

import com.google.cloud.Timestamp;

/**
 * Contains information about the current state
 * of the Spanner partition
 */
public class PartitionState implements Comparable<PartitionState> {
    private final String token;
    private final Timestamp startTimestamp;
    private final Timestamp endTimestamp;
    private final PartitionStateEnum state;
    private final Set<String> parents;
    private final String assigneeTaskUid;

    private final Timestamp finishedTimestamp;

    public PartitionState(final String token, final Timestamp startTimestamp,
                          final Timestamp endTimestamp, final PartitionStateEnum state,
                          final Set<String> parents, final String assigneeTaskUid, final Timestamp finishedTimestamp) {
        this.token = token;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.state = state;
        this.parents = parents;
        this.assigneeTaskUid = assigneeTaskUid;
        this.finishedTimestamp = finishedTimestamp;
    }

    public static class PartitionStateBuilder {

        private String token;

        private Timestamp startTimestamp;

        private Timestamp endTimestamp;

        private PartitionStateEnum state;

        private Set<String> parents;

        private String assigneeTaskUid;

        private Timestamp finishedTimestamp;

        PartitionStateBuilder() {
        }

        public PartitionState.PartitionStateBuilder token(final String token) {
            this.token = token;
            return this;
        }

        public PartitionState.PartitionStateBuilder startTimestamp(final Timestamp startTimestamp) {
            this.startTimestamp = startTimestamp;
            return this;
        }

        public PartitionState.PartitionStateBuilder endTimestamp(final Timestamp endTimestamp) {
            this.endTimestamp = endTimestamp;
            return this;
        }

        public PartitionState.PartitionStateBuilder state(final PartitionStateEnum state) {
            this.state = state;
            return this;
        }

        public PartitionState.PartitionStateBuilder parents(final Set<String> parents) {
            this.parents = parents;
            return this;
        }

        public PartitionState.PartitionStateBuilder assigneeTaskUid(final String assigneeTaskUid) {
            this.assigneeTaskUid = assigneeTaskUid;
            return this;
        }

        public PartitionState.PartitionStateBuilder finishedTimestamp(final Timestamp finishedTime) {
            this.finishedTimestamp = finishedTime;
            return this;
        }

        public PartitionState build() {
            return new PartitionState(this.token, this.startTimestamp,
                    this.endTimestamp, this.state, this.parents,
                    this.assigneeTaskUid, this.finishedTimestamp);
        }

    }

    public static PartitionState.PartitionStateBuilder builder() {
        return new PartitionState.PartitionStateBuilder();
    }

    public PartitionState.PartitionStateBuilder toBuilder() {
        return new PartitionStateBuilder()
                .token(this.token)
                .startTimestamp(this.startTimestamp)
                .endTimestamp(this.endTimestamp)
                .state(this.state)
                .parents(this.parents)
                .assigneeTaskUid(this.assigneeTaskUid)
                .finishedTimestamp(this.finishedTimestamp);
    }

    public String getToken() {
        return this.token;
    }

    public Timestamp getStartTimestamp() {
        return this.startTimestamp;
    }

    public Timestamp getEndTimestamp() {
        return this.endTimestamp;
    }

    public PartitionStateEnum getState() {
        return this.state;
    }

    public Set<String> getParents() {
        return this.parents;
    }

    public String getAssigneeTaskUid() {
        return this.assigneeTaskUid;
    }

    public Timestamp getFinishedTimestamp() {
        return finishedTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionState that = (PartitionState) o;
        return Objects.equals(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token);
    }

    @Override
    public int compareTo(PartitionState partitionState) {
        if (!Objects.equals(partitionState.getToken(), token)) {
            return token.compareTo(partitionState.getToken());
        }
        return state.compareTo(partitionState.state);
    }

    @Override
    public String toString() {
        return "PartitionState{" +
                "token='" + token + '\'' +
                ", startTimestamp=" + startTimestamp +
                ", endTimestamp=" + endTimestamp +
                ", state=" + state +
                ", parents=" + parents +
                ", assigneeTaskUid='" + assigneeTaskUid + '\'' +
                ", finishedTimestamp=" + finishedTimestamp +
                '}';
    }
}
