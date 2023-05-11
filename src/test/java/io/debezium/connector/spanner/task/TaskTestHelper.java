/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

public class TaskTestHelper {
    private TaskTestHelper() {
    }

    public static TaskState generateTaskStateWithPartitions(List<PartitionState> partitions) {
        return TaskState.builder()
                .taskUid(UUID.randomUUID().toString())
                .partitions(partitions)
                .sharedPartitions(List.of())
                .build();
    }

    public static TaskState generateTaskStateWithPartitions(String taskUid, List<PartitionState> partitions, List<PartitionState> sharedPartitions) {
        return TaskState.builder()
                .taskUid(taskUid)
                .partitions(partitions)
                .sharedPartitions(sharedPartitions)
                .build();
    }

    public static TaskState generateTaskStateWithPartitions(
                                                            String taskUid, String consumerId, long rebalanceGenerationId, long stateTimestamp,
                                                            List<PartitionState> partitions, List<PartitionState> sharedPartitions) {
        return TaskState.builder()
                .taskUid(taskUid)
                .consumerId(consumerId)
                .rebalanceGenerationId(rebalanceGenerationId)
                .stateTimestamp(stateTimestamp)
                .partitions(partitions)
                .sharedPartitions(sharedPartitions)
                .build();
    }

    public static TaskState generateTaskStateWithRandomPartitions(int partitionsCount, int sharedPartitionsCount) {
        return TaskState.builder()
                .taskUid(UUID.randomUUID().toString())
                .partitions(generateRandomPartitions(partitionsCount))
                .sharedPartitions(generateRandomPartitions(sharedPartitionsCount))
                .build();
    }

    public static TaskSyncEvent createTaskSyncEvent(TaskState... taskStates) {
        return TaskSyncEvent.builder()
                .taskStates(createTaskStateMap(taskStates)).build();
    }

    public static TaskSyncEvent createTaskSyncEvent(
                                                    String taskUid, String consumerId,
                                                    long rebalanceGenerationId, MessageTypeEnum messageType, TaskState... taskStates) {
        return TaskSyncEvent.builder()
                .taskUid(taskUid)
                .consumerId(consumerId)
                .rebalanceGenerationId(rebalanceGenerationId)
                .messageType(messageType)
                .taskStates(createTaskStateMap(taskStates)).build();
    }

    public static Map<String, TaskState> createTaskStateMap(TaskState... taskStates) {
        return Stream.of(taskStates)
                .collect(toUnmodifiableMap(TaskState::getTaskUid, identity()));
    }

    public static List<PartitionState> generateRandomPartitions(int count) {
        return Stream.generate(TaskTestHelper::generateRandomPartition).limit(count).collect(toUnmodifiableList());
    }

    public static PartitionState generateRandomPartition() {
        return PartitionState.builder()
                .token(UUID.randomUUID().toString())
                .build();
    }

    public static List<PartitionState> generatePartitions(int count, Supplier<PartitionState> partitionStateSupplier) {
        return Stream.generate(partitionStateSupplier).limit(count).collect(toUnmodifiableList());
    }

    public static PartitionState findPartitionStateByToken(Collection<PartitionState> partitions, String token) {
        return partitions.stream().filter(p -> p.getToken().equals(token)).findFirst().orElseThrow();
    }

    public static List<PartitionState> findPartitionStatesByAssignee(List<PartitionState> partitions, String assigneeTaskUid) {
        return partitions.stream().filter(p -> p.getAssigneeTaskUid().equals(assigneeTaskUid)).collect(toList());
    }

    public static List<String> extractTokens(List<PartitionState> partitions) {
        return partitions.stream().map(PartitionState::getToken).collect(toList());
    }
}