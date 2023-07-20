/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static java.util.Collections.emptyList;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * Represents state of the current task and collected
 * incremental states of other tasks taken from
 * the Sync Topic
 */
public class TaskSyncContext {
    private static final Logger LOGGER = getLogger(TaskSyncContext.class);

    private final String taskUid;
    private final RebalanceState rebalanceState;
    private final String consumerId;
    private final long rebalanceGenerationId;
    private final EpochOffsetHolder epochOffsetHolder;
    private final long currentKafkaRecordOffset;
    private final boolean isLeader;
    private final long createdTimestamp;
    private final Map<String, TaskState> taskStates;
    private final TaskState currentTaskState;

    private final Timestamp databaseSchemaTimestamp;

    private final boolean finished;

    private final boolean initialized;

    public Map<String, TaskState> getAllTaskStates() {
        Map<String, TaskState> taskStateMap = new HashMap<>(this.taskStates);
        taskStateMap.put(currentTaskState.getTaskUid(), currentTaskState.toBuilder()
                .consumerId(consumerId)
                .rebalanceGenerationId(rebalanceGenerationId)
                .stateTimestamp(Instant.now().toEpochMilli()).build());
        return Map.copyOf(taskStateMap);
    }

    public TaskSyncEvent buildTaskSyncEvent() {
        return buildTaskSyncEvent(MessageTypeEnum.REGULAR);
    }

    public TaskSyncEvent buildTaskSyncEvent(MessageTypeEnum messageType) {
        return TaskSyncEvent.builder().epochOffset(this.epochOffsetHolder.getEpochOffset()).taskStates(
                this.getAllTaskStates())
                .taskUid(this.getTaskUid())
                .consumerId(this.getConsumerId())
                .rebalanceGenerationId(this.getRebalanceGenerationId())
                .messageTimestamp(this.getCreatedTimestamp())
                .messageType(messageType)
                .databaseSchemaTimestamp(databaseSchemaTimestamp)
                .build();
    }

    public static TaskSyncContext getInitialContext(String taskUid, SpannerConnectorConfig connectorConfig) {
        long now = Instant.now().toEpochMilli();
        return TaskSyncContext.builder()
                .taskUid(taskUid)
                .consumerId("")
                .databaseSchemaTimestamp(connectorConfig.startTime())
                .rebalanceGenerationId(-2)
                .rebalanceState(RebalanceState.START_INITIAL_SYNC)
                .createdTimestamp(now)
                .currentTaskState(
                        TaskState.builder()
                                .taskUid(taskUid)
                                .consumerId("")
                                .partitions(emptyList())
                                .sharedPartitions(emptyList())
                                .stateTimestamp(now)
                                .build())
                .build();
    }

    private static EpochOffsetHolder defaultEpochOffsetHolder() {
        return new EpochOffsetHolder(0);
    }

    private static boolean defaultIsLeader() {
        return false;
    }

    private static long defaultCreatedTimestamp() {
        return Instant.now().toEpochMilli();
    }

    private static Map<String, TaskState> defaultTaskStates() {
        return Map.of();
    }

    TaskSyncContext(final String taskUid,
                    final RebalanceState rebalanceState,
                    final String consumerId,
                    final long rebalanceGenerationId,
                    final EpochOffsetHolder epochOffsetHolder,
                    final long currentKafkaRecordOffset,
                    final boolean isLeader,
                    final long createdTimestamp,
                    final Map<String, TaskState> taskStates,
                    final TaskState currentTaskState,
                    Timestamp databaseSchemaTimestamp,
                    boolean finished,
                    boolean initialized) {
        this.taskUid = taskUid;
        this.rebalanceState = rebalanceState;
        this.consumerId = consumerId;
        this.rebalanceGenerationId = rebalanceGenerationId;
        this.epochOffsetHolder = epochOffsetHolder;
        this.currentKafkaRecordOffset = currentKafkaRecordOffset;
        this.isLeader = isLeader;
        this.createdTimestamp = createdTimestamp;
        this.taskStates = taskStates;
        this.currentTaskState = currentTaskState;
        this.databaseSchemaTimestamp = databaseSchemaTimestamp;
        this.finished = finished;
        this.initialized = initialized;
    }

    public static class TaskSyncContextBuilder {
        private String taskUid;
        private RebalanceState rebalanceState;
        private String consumerId;
        private long rebalanceGenerationId;
        private boolean epochOffsetHolderSet;
        private EpochOffsetHolder epochOffsetHolderValue;
        private long currentKafkaRecordOffset;
        private boolean isLeaderSet;
        private boolean isLeaderValue;
        private boolean createdTimestampSet;
        private long createdTimestampValue;
        private boolean taskStatesSet;
        private Map<String, TaskState> taskStatesValue;
        private TaskState currentTaskState;

        private Timestamp databaseSchemaTimestamp;

        private boolean finished;

        private boolean initialized;

        TaskSyncContextBuilder() {
        }

        public TaskSyncContext.TaskSyncContextBuilder taskUid(final String taskUid) {
            this.taskUid = taskUid;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder rebalanceState(final RebalanceState rebalanceState) {
            this.rebalanceState = rebalanceState;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder consumerId(final String consumerId) {
            this.consumerId = consumerId;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder rebalanceGenerationId(final long rebalanceGenerationId) {
            this.rebalanceGenerationId = rebalanceGenerationId;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder epochOffsetHolder(final EpochOffsetHolder epochOffsetHolder) {
            this.epochOffsetHolderValue = epochOffsetHolder;
            epochOffsetHolderSet = true;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder currentKafkaRecordOffset(final long currentKafkaRecordOffset) {
            this.currentKafkaRecordOffset = currentKafkaRecordOffset;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder isLeader(final boolean isLeader) {
            this.isLeaderValue = isLeader;
            isLeaderSet = true;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder createdTimestamp(final long createdTimestamp) {
            this.createdTimestampValue = createdTimestamp;
            createdTimestampSet = true;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder taskStates(final Map<String, TaskState> taskStates) {
            this.taskStatesValue = taskStates;
            taskStatesSet = true;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder currentTaskState(final TaskState currentTaskState) {
            this.currentTaskState = currentTaskState;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder databaseSchemaTimestamp(final Timestamp databaseSchemaTimestamp) {
            this.databaseSchemaTimestamp = databaseSchemaTimestamp;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder finished(final boolean finished) {
            this.finished = finished;
            return this;
        }

        public TaskSyncContext.TaskSyncContextBuilder initialized(final boolean initialized) {
            this.initialized = initialized;
            return this;
        }

        public TaskSyncContext build() {
            EpochOffsetHolder epochOffsetHolderValue = this.epochOffsetHolderValue;
            if (!this.epochOffsetHolderSet) {
                epochOffsetHolderValue = TaskSyncContext.defaultEpochOffsetHolder();
            }

            boolean isLeaderValue = this.isLeaderValue;
            if (!this.isLeaderSet) {
                isLeaderValue = TaskSyncContext.defaultIsLeader();
            }

            long createdTimestampValue = this.createdTimestampValue;
            if (!this.createdTimestampSet) {
                createdTimestampValue = TaskSyncContext.defaultCreatedTimestamp();
            }
            Map<String, TaskState> taskStatesValue = this.taskStatesValue;

            if (!this.taskStatesSet) {
                taskStatesValue = TaskSyncContext.defaultTaskStates();
            }

            return new TaskSyncContext(this.taskUid, this.rebalanceState, this.consumerId,
                    this.rebalanceGenerationId, epochOffsetHolderValue,
                    this.currentKafkaRecordOffset, isLeaderValue, createdTimestampValue,
                    taskStatesValue, this.currentTaskState, databaseSchemaTimestamp, finished, initialized);
        }

        @Override
        public String toString() {
            return "TaskSyncContext.TaskSyncContextBuilder(taskUid=" + this.taskUid +
                    ", rebalanceState=" + this.rebalanceState +
                    ", consumerId=" + this.consumerId +
                    ", rebalanceGenerationId=" + this.rebalanceGenerationId +
                    ", epochOffsetHolder=" + this.epochOffsetHolderValue +
                    ", currentKafkaRecordOffset=" + this.currentKafkaRecordOffset +
                    ", isLeader=" + this.isLeaderValue +
                    ", createdTimestamp=" + this.createdTimestampValue +
                    ", taskStates=" + this.taskStatesValue +
                    ", currentTaskState=" + this.currentTaskState + ")";
        }
    }

    public static TaskSyncContext.TaskSyncContextBuilder builder() {
        return new TaskSyncContext.TaskSyncContextBuilder();
    }

    public TaskSyncContext.TaskSyncContextBuilder toBuilder() {
        return new TaskSyncContextBuilder()
                .taskUid(this.taskUid)
                .rebalanceState(this.rebalanceState)
                .consumerId(this.consumerId)
                .rebalanceGenerationId(this.rebalanceGenerationId)
                .epochOffsetHolder(this.epochOffsetHolder)
                .currentKafkaRecordOffset(this.currentKafkaRecordOffset)
                .isLeader(this.isLeader)
                .createdTimestamp(this.createdTimestamp)
                .taskStates(this.taskStates)
                .currentTaskState(this.currentTaskState)
                .databaseSchemaTimestamp(this.databaseSchemaTimestamp)
                .finished(this.finished)
                .initialized(this.initialized);
    }

    public String getTaskUid() {
        return this.taskUid;
    }

    public RebalanceState getRebalanceState() {
        return this.rebalanceState;
    }

    public String getConsumerId() {
        return this.consumerId;
    }

    public long getRebalanceGenerationId() {
        return this.rebalanceGenerationId;
    }

    public EpochOffsetHolder getEpochOffsetHolder() {
        return this.epochOffsetHolder;
    }

    public long getCurrentKafkaRecordOffset() {
        return this.currentKafkaRecordOffset;
    }

    public boolean isLeader() {
        return this.isLeader;
    }

    public long getCreatedTimestamp() {
        return this.createdTimestamp;
    }

    public Map<String, TaskState> getTaskStates() {
        return this.taskStates;
    }

    public TaskState getCurrentTaskState() {
        return this.currentTaskState;
    }

    public Timestamp getDatabaseSchemaTimestamp() {
        return this.databaseSchemaTimestamp;
    }

    public boolean isFinished() {
        return finished;
    }

    public boolean isInitialized() {
        return this.initialized;
    }

    // Debug function used to check if there is any partiton or shared partition duplication
    // inside the TaskSyncContext.
    public boolean checkDuplication(boolean printOffsets, String loggingString) {

        // Filter out all of the FINISHED or REMOVED partitions.
        Map<String, List<PartitionState>> partitionsMap = getAllTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .filter(
                        partitionState -> !partitionState.getState().equals(PartitionStateEnum.FINISHED)
                                && !partitionState.getState().equals(PartitionStateEnum.REMOVED))
                .collect(Collectors.groupingBy(PartitionState::getToken));

        int numPartitions = partitionsMap.size();

        // Check that there are no duplicate partitions in the partitions map.
        Set<String> duplicatesInPartitions = checkDuplicationInMap(partitionsMap);
        if (!duplicatesInPartitions.isEmpty()) {
            if (printOffsets) {
                LOGGER.warn(
                        "task: {}, logging {}, taskSyncContext: found duplication in partitionsMap with size {}: {}", getTaskUid(), loggingString, numPartitions,
                        duplicatesInPartitions);
            }
            return true;
        }

        Map<String, PartitionState> partitions = partitionsMap.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().get(0)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, List<PartitionState>> sharedPartitionsMap = getAllTaskStates().values().stream()
                .flatMap(taskState -> taskState.getSharedPartitions().stream())
                .filter(partitionState -> !partitions.containsKey(partitionState.getToken()))
                .collect(Collectors.groupingBy(PartitionState::getToken));

        int numSharedPartitions = sharedPartitionsMap.size();

        // Check that there are no duplicate partitions in the shared partitions map.
        Set<String> duplicatesInSharedPartitions = checkDuplicationInMap(sharedPartitionsMap);
        if (!duplicatesInSharedPartitions.isEmpty()) {
            if (printOffsets) {
                LOGGER.warn(
                        "task: {}, logging {}, taskSyncContext: found duplication in sharedPartitionsMap with size {}: {}",
                        getTaskUid(), loggingString, numSharedPartitions, duplicatesInSharedPartitions);
            }
            return true;
        }
        if (printOffsets) {
            LOGGER.warn(
                    "task: {}, logging {}, taskSyncContext: counted num partitions {} and num shared partitions {} ",
                    getTaskUid(), loggingString, numPartitions,
                    numSharedPartitions);
        }
        return false;
    }

    @Override
    public String toString() {
        return "TaskSyncContext(taskUid=" + this.getTaskUid() +
                ", rebalanceState=" + this.getRebalanceState() +
                ", consumerId=" + this.getConsumerId() +
                ", rebalanceGenerationId=" + this.getRebalanceGenerationId() +
                ", epochOffsetHolder=" + this.getEpochOffsetHolder() +
                ", currentKafkaRecordOffset=" + this.getCurrentKafkaRecordOffset() +
                ", isLeader=" + this.isLeader() +
                ", createdTimestamp=" + this.getCreatedTimestamp() +
                ", taskStates=" + this.getTaskStates() +
                ", currentTaskState=" + this.getCurrentTaskState() + ")";
    }

    private Set<String> checkDuplicationInMap(Map<String, List<PartitionState>> map) {
        return map.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }
}
