/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the internal state of connector task
 */
public class TaskState {
    private final String taskUid;
    private final String consumerId;
    private final long rebalanceGenerationId;
    private final long stateTimestamp;
    private final Map<String, PartitionState> partitions;
    private final Map<String, PartitionState> sharedPartitions;

    public static class TaskStateBuilder {

        private String taskUid;

        private String consumerId;

        private long rebalanceGenerationId;

        private long stateTimestamp;

        private Map<String, PartitionState> partitions;

        private Map<String, PartitionState> sharedPartitions;

        TaskStateBuilder() {
        }

        public TaskState.TaskStateBuilder taskUid(final String taskUid) {
            this.taskUid = taskUid;
            return this;
        }

        public TaskState.TaskStateBuilder consumerId(final String consumerId) {
            this.consumerId = consumerId;
            return this;
        }

        public TaskState.TaskStateBuilder rebalanceGenerationId(final long rebalanceGenerationId) {
            this.rebalanceGenerationId = rebalanceGenerationId;
            return this;
        }

        public TaskState.TaskStateBuilder stateTimestamp(final long stateTimestamp) {
            this.stateTimestamp = stateTimestamp;
            return this;
        }

        public TaskState.TaskStateBuilder partitions(final List<PartitionState> partitions) {
            this.partitions = partitions.stream().collect(Collectors.toMap(PartitionState::getToken, Function.identity()));
            return this;
        }

        public TaskState.TaskStateBuilder sharedPartitions(final List<PartitionState> sharedPartitions) {
            this.sharedPartitions = sharedPartitions.stream().collect(Collectors.toMap(PartitionState::getToken, Function.identity()));
            return this;
        }

        public TaskState.TaskStateBuilder partitionsMap(final Map<String, PartitionState> partitions) {
            this.partitions = partitions;
            return this;
        }

        public TaskState.TaskStateBuilder sharedPartitionsMap(final Map<String, PartitionState> sharedPartitions) {
            this.sharedPartitions = sharedPartitions;
            return this;
        }

        public TaskState build() {
            return new TaskState(this.taskUid, this.consumerId,
                    this.rebalanceGenerationId, this.stateTimestamp,
                    this.partitions, this.sharedPartitions);
        }

        @Override
        public String toString() {
            return "TaskState.TaskStateBuilder(taskUid=" + this.taskUid
                    + ", consumerId=" + this.consumerId
                    + ", rebalanceGenerationId=" + this.rebalanceGenerationId
                    + ", stateTimestamp=" + this.stateTimestamp
                    + ", partitions=" + this.partitions
                    + ", sharedPartitions=" + this.sharedPartitions + ")";
        }
    }

    public static TaskState.TaskStateBuilder builder() {
        return new TaskState.TaskStateBuilder();
    }

    public TaskState.TaskStateBuilder toBuilder() {
        return new TaskState.TaskStateBuilder()
                .taskUid(this.taskUid).consumerId(this.consumerId)
                .rebalanceGenerationId(this.rebalanceGenerationId)
                .stateTimestamp(this.stateTimestamp)
                .partitionsMap(this.partitions)
                .sharedPartitionsMap(this.sharedPartitions);
    }

    public TaskState(final String taskUid,
                     final String consumerId,
                     final long rebalanceGenerationId,
                     final long stateTimestamp,
                     final Map<String, PartitionState> partitions,
                     final Map<String, PartitionState> sharedPartitions) {
        this.taskUid = taskUid;
        this.consumerId = consumerId;
        this.rebalanceGenerationId = rebalanceGenerationId;
        this.stateTimestamp = stateTimestamp;
        this.partitions = partitions;
        this.sharedPartitions = sharedPartitions;
    }

    public String getTaskUid() {
        return this.taskUid;
    }

    public String getConsumerId() {
        return this.consumerId;
    }

    public long getRebalanceGenerationId() {
        return this.rebalanceGenerationId;
    }

    public long getStateTimestamp() {
        return this.stateTimestamp;
    }

    public Map<String, PartitionState> getPartitionsMap() {
        return this.partitions;
    }

    public Map<String, PartitionState> getSharedPartitionsMap() {
        return this.sharedPartitions;
    }

    public Collection<PartitionState> getPartitions() {
        return this.partitions.values();
    }

    public Collection<PartitionState> getSharedPartitions() {
        return this.sharedPartitions.values();
    }

    @Override
    public String toString() {
        return "TaskState(taskUid=" + this.getTaskUid() +
                ", consumerId=" + this.getConsumerId() +
                ", rebalanceGenerationId=" + this.getRebalanceGenerationId() +
                ", stateTimestamp=" + this.getStateTimestamp() +
                ", partitions=" + this.getPartitions() +
                ", sharedPartitions=" + this.getSharedPartitions() + ")";
    }
}
