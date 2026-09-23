/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.PartitionKey;

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

    private final String originParent;

    private final MoveInState moveInState;

    private final List<MoveOutState> moveOutStates;

    private final Timestamp processedTimestamp;

    private final String lastBoundaryRecordSequence;

    private final String tvfName;

    public PartitionState(final String token, final Timestamp startTimestamp,
                          final Timestamp endTimestamp, final PartitionStateEnum state,
                          final Set<String> parents, final String assigneeTaskUid, final Timestamp finishedTimestamp,
                          final String originParent) {
        this(token, startTimestamp, endTimestamp, state, parents, assigneeTaskUid, finishedTimestamp, originParent, null, null, null, null);
    }

    public PartitionState(final String token, final Timestamp startTimestamp,
                          final Timestamp endTimestamp, final PartitionStateEnum state,
                          final Set<String> parents, final String assigneeTaskUid, final Timestamp finishedTimestamp,
                          final String originParent, final MoveInState moveInState, final List<MoveOutState> moveOutStates) {
        this(token, startTimestamp, endTimestamp, state, parents, assigneeTaskUid, finishedTimestamp, originParent, moveInState, moveOutStates, null, null);
    }

    public PartitionState(final String token, final Timestamp startTimestamp,
                          final Timestamp endTimestamp, final PartitionStateEnum state,
                          final Set<String> parents, final String assigneeTaskUid, final Timestamp finishedTimestamp,
                          final String originParent, final MoveInState moveInState, final List<MoveOutState> moveOutStates,
                          final Timestamp processedTimestamp) {
        this(token, startTimestamp, endTimestamp, state, parents, assigneeTaskUid, finishedTimestamp, originParent, moveInState, moveOutStates, processedTimestamp, null);
    }

    public PartitionState(final String token, final Timestamp startTimestamp,
                          final Timestamp endTimestamp, final PartitionStateEnum state,
                          final Set<String> parents, final String assigneeTaskUid, final Timestamp finishedTimestamp,
                          final String originParent, final MoveInState moveInState, final List<MoveOutState> moveOutStates,
                          final Timestamp processedTimestamp, final String lastBoundaryRecordSequence) {
        this(token, startTimestamp, endTimestamp, state, parents, assigneeTaskUid, finishedTimestamp, originParent, moveInState, moveOutStates,
                processedTimestamp, lastBoundaryRecordSequence, null);
    }

    public PartitionState(final String token, final Timestamp startTimestamp,
                          final Timestamp endTimestamp, final PartitionStateEnum state,
                          final Set<String> parents, final String assigneeTaskUid, final Timestamp finishedTimestamp,
                          final String originParent, final MoveInState moveInState, final List<MoveOutState> moveOutStates,
                          final Timestamp processedTimestamp, final String lastBoundaryRecordSequence, final String tvfName) {
        this.token = token;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.state = state;
        this.parents = parents;
        this.assigneeTaskUid = assigneeTaskUid;
        this.finishedTimestamp = finishedTimestamp;
        this.originParent = originParent;
        this.moveInState = moveInState;
        this.moveOutStates = moveOutStates == null ? List.of() : moveOutStates;
        this.processedTimestamp = processedTimestamp;
        this.lastBoundaryRecordSequence = lastBoundaryRecordSequence;
        this.tvfName = tvfName;
    }

    public static class PartitionStateBuilder {

        private String token;

        private Timestamp startTimestamp;

        private Timestamp endTimestamp;

        private PartitionStateEnum state;

        private Set<String> parents;

        private String assigneeTaskUid;

        private Timestamp finishedTimestamp;

        private String originParent;

        private MoveInState moveInState;

        private List<MoveOutState> moveOutStates;

        private Timestamp processedTimestamp;

        private String lastBoundaryRecordSequence;

        private String tvfName;

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

        public PartitionState.PartitionStateBuilder originParent(final String originParent) {
            this.originParent = originParent;
            return this;
        }

        public PartitionState.PartitionStateBuilder moveInState(final MoveInState moveInState) {
            this.moveInState = moveInState;
            return this;
        }

        public PartitionState.PartitionStateBuilder moveOutStates(final List<MoveOutState> moveOutStates) {
            this.moveOutStates = moveOutStates;
            return this;
        }

        public PartitionState.PartitionStateBuilder processedTimestamp(final Timestamp processedTimestamp) {
            this.processedTimestamp = processedTimestamp;
            return this;
        }

        public PartitionState.PartitionStateBuilder lastBoundaryRecordSequence(final String lastBoundaryRecordSequence) {
            this.lastBoundaryRecordSequence = lastBoundaryRecordSequence;
            return this;
        }

        public PartitionState.PartitionStateBuilder tvfName(final String tvfName) {
            this.tvfName = tvfName;
            return this;
        }

        public PartitionState build() {
            return new PartitionState(this.token, this.startTimestamp,
                    this.endTimestamp, this.state, this.parents,
                    this.assigneeTaskUid, this.finishedTimestamp, this.originParent,
                    this.moveInState, this.moveOutStates, this.processedTimestamp,
                    this.lastBoundaryRecordSequence, this.tvfName);
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
                .finishedTimestamp(this.finishedTimestamp)
                .originParent(this.originParent)
                .moveInState(this.moveInState)
                .moveOutStates(this.moveOutStates)
                .processedTimestamp(this.processedTimestamp)
                .lastBoundaryRecordSequence(this.lastBoundaryRecordSequence)
                .tvfName(this.tvfName);
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

    public String getOriginParent() {
        return originParent;
    }

    public MoveInState getMoveInState() {
        return moveInState;
    }

    public List<MoveOutState> getMoveOutStates() {
        return moveOutStates;
    }

    public Timestamp getProcessedTimestamp() {
        return processedTimestamp;
    }

    public String getLastBoundaryRecordSequence() {
        return lastBoundaryRecordSequence;
    }

    /**
     * The name of the placement table-valued function (TVF) this partition originated from, when the
     * change stream is configured with {@code gcp.spanner.placement.tvf.names}, or null otherwise.
     */
    public String getTvfName() {
        return tvfName;
    }

    public PartitionKey getKey() {
        return new PartitionKey(token, tvfName);
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
        return Objects.equals(token, that.token) && Objects.equals(tvfName, that.tvfName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, tvfName);
    }

    @Override
    public int compareTo(PartitionState partitionState) {
        int keyComparison = getKey().compareTo(partitionState.getKey());
        return keyComparison != 0 ? keyComparison : state.compareTo(partitionState.state);
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
                ", originParent='" + originParent + '\'' +
                ", moveInState=" + moveInState +
                ", moveOutStates=" + moveOutStates +
                ", processedTimestamp=" + processedTimestamp +
                ", lastBoundaryRecordSequence='" + lastBoundaryRecordSequence + '\'' +
                ", tvfName='" + tvfName + '\'' +
                '}';
    }
}
