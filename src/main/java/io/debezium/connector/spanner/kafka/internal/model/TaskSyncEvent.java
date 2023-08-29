/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.cloud.Timestamp;

/**
 * Tasks are exchanging information about their internal states,
 * using TaskSyncEvent published to  the Sync topic.
 */

public class TaskSyncEvent {
    private final String taskUid;
    private final String consumerId;
    private final long messageTimestamp;
    private final MessageTypeEnum messageType;
    private final long rebalanceGenerationId;
    private final long epochOffset;
    private final Map<String, TaskState> taskStates;

    public static class TaskSyncEventBuilder {

        private String taskUid;

        private String consumerId;

        private long messageTimestamp;

        private MessageTypeEnum messageType;

        private long rebalanceGenerationId;

        private long epochOffset;

        private Map<String, TaskState> taskStates;

        private Timestamp databaseSchemaTimestamp;

        TaskSyncEventBuilder() {
        }

        public TaskSyncEvent.TaskSyncEventBuilder taskUid(final String taskUid) {
            this.taskUid = taskUid;
            return this;
        }

        public TaskSyncEvent.TaskSyncEventBuilder consumerId(final String consumerId) {
            this.consumerId = consumerId;
            return this;
        }

        public TaskSyncEvent.TaskSyncEventBuilder messageTimestamp(final long messageTimestamp) {
            this.messageTimestamp = messageTimestamp;
            return this;
        }

        public TaskSyncEvent.TaskSyncEventBuilder messageType(final MessageTypeEnum messageType) {
            this.messageType = messageType;
            return this;
        }

        public TaskSyncEvent.TaskSyncEventBuilder rebalanceGenerationId(final long rebalanceGenerationId) {
            this.rebalanceGenerationId = rebalanceGenerationId;
            return this;
        }

        public TaskSyncEvent.TaskSyncEventBuilder epochOffset(final long epochOffset) {
            this.epochOffset = epochOffset;
            return this;
        }

        public TaskSyncEvent.TaskSyncEventBuilder taskStates(final Map<String, TaskState> taskStates) {
            this.taskStates = taskStates;
            return this;
        }

        public TaskSyncEvent.TaskSyncEventBuilder databaseSchemaTimestamp(final Timestamp databaseSchemaTimestamp) {
            this.databaseSchemaTimestamp = databaseSchemaTimestamp;
            return this;
        }

        public TaskSyncEvent build() {
            return new TaskSyncEvent(this.taskUid, this.consumerId, this.messageTimestamp, this.messageType, this.rebalanceGenerationId, this.epochOffset,
                    this.taskStates);
        }

        @Override
        public String toString() {
            return "TaskSyncEvent.TaskSyncEventBuilder(taskUid=" + this.taskUid
                    + ", consumerId=" + this.consumerId + ", messageTimestamp="
                    + this.messageTimestamp + ", messageType="
                    + this.messageType + ", rebalanceGenerationId="
                    + this.rebalanceGenerationId + ", epochOffset="
                    + this.epochOffset + ", taskStates=" + this.taskStates + ")";
        }
    }

    public static TaskSyncEvent.TaskSyncEventBuilder builder() {
        return new TaskSyncEvent.TaskSyncEventBuilder();
    }

    public TaskSyncEvent.TaskSyncEventBuilder toBuilder() {
        return new TaskSyncEvent.TaskSyncEventBuilder()
                .taskUid(this.taskUid).consumerId(this.consumerId)
                .messageTimestamp(this.messageTimestamp)
                .messageType(this.messageType).rebalanceGenerationId(this.rebalanceGenerationId)
                .epochOffset(this.epochOffset).taskStates(this.taskStates);
    }

    public TaskSyncEvent(final String taskUid,
                         final String consumerId,
                         final long messageTimestamp,
                         final MessageTypeEnum messageType,
                         final long rebalanceGenerationId,
                         final long epochOffset,
                         final Map<String, TaskState> taskStates) {
        this.taskUid = taskUid;
        this.consumerId = consumerId;
        this.messageTimestamp = messageTimestamp;
        this.messageType = messageType;
        this.rebalanceGenerationId = rebalanceGenerationId;
        this.epochOffset = epochOffset;
        this.taskStates = taskStates;
    }

    public String getTaskUid() {
        return this.taskUid;
    }

    public String getConsumerId() {
        return this.consumerId;
    }

    public long getMessageTimestamp() {
        return this.messageTimestamp;
    }

    public MessageTypeEnum getMessageType() {
        return this.messageType;
    }

    public long getRebalanceGenerationId() {
        return this.rebalanceGenerationId;
    }

    public long getEpochOffset() {
        return this.epochOffset;
    }

    public Map<String, TaskState> getTaskStates() {
        return this.taskStates;
    }

    public int getNumPartitions() {
        Map<String, List<PartitionState>> partitionsMap = getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .filter(
                        partitionState -> !partitionState.getState().equals(PartitionStateEnum.FINISHED)
                                && !partitionState.getState().equals(PartitionStateEnum.REMOVED))
                .collect(Collectors.groupingBy(PartitionState::getToken));

        return partitionsMap.size();
    }

    public int getNumSharedPartitions() {
        Map<String, List<PartitionState>> partitionsMap = getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .filter(
                        partitionState -> !partitionState.getState().equals(PartitionStateEnum.FINISHED)
                                && !partitionState.getState().equals(PartitionStateEnum.REMOVED))
                .collect(Collectors.groupingBy(PartitionState::getToken));

        Map<String, PartitionState> partitions = partitionsMap.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().get(0)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, List<PartitionState>> sharedPartitionsMap = getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getSharedPartitions().stream())
                .filter(partitionState -> !partitions.containsKey(partitionState.getToken()))
                .collect(Collectors.groupingBy(PartitionState::getToken));

        return sharedPartitionsMap.size();
    }

    @Override
    public String toString() {
        return "TaskSyncEvent(taskUid=" + this.getTaskUid() +
                ", consumerId=" + this.getConsumerId() +
                ", messageTimestamp=" + this.getMessageTimestamp() +
                ", messageType=" + this.getMessageType() +
                ", rebalanceGenerationId=" + this.getRebalanceGenerationId() +
                ", epochOffset=" + this.getEpochOffset() +
                ", taskStates=" + this.getTaskStates() + ")";
    }
}
