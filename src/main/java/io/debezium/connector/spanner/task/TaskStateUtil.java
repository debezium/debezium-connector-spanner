/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;

/**
 * Utility for grouping and filtering tasks,
 * which survived and not after the Rebalance Event
 */
public class TaskStateUtil {
    private TaskStateUtil() {
    }

    public static Map<String, TaskState> filterSurvivedTasksStates(Map<String, TaskState> taskStates, Collection<String> survivedTasksUids) {
        return taskStates
                .entrySet()
                .stream()
                .filter(e -> survivedTasksUids.contains(e.getKey()))
                .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<Boolean, Map<String, TaskState>> splitSurvivedAndObsoleteTaskStates(Map<String, TaskState> taskStates, Collection<String> survivedTasksUids) {
        return taskStates
                .entrySet()
                .stream()
                .collect(partitioningBy(
                        e -> survivedTasksUids.contains(e.getKey()),
                        toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public static long numOwnedAndAssignedPartitions(TaskSyncContext taskSyncContext) {
        Stream<PartitionState> assignedPartitions = taskSyncContext
                .getTaskStates()
                .values()
                .stream()
                .flatMap(t -> t.getSharedPartitions().stream())
                .filter(p -> inProgressPartitionState(p.getState()))
                .filter(p -> p.getAssigneeTaskUid().equals(taskSyncContext.getTaskUid()));

        Stream<PartitionState> ownedPartitions = taskSyncContext.getCurrentTaskState().getPartitions().stream()
                .filter(p -> inProgressPartitionState(p.getState()));

        return Stream.concat(ownedPartitions, assignedPartitions).distinct().count();
    }

    public static int totalInProgressPartitions(TaskSyncContext taskSyncContext) {
        return allFilteredPartitionTokens(taskSyncContext, partition -> inProgressPartitionState(partition.getState())).size();
    }

    public static int totalFinishedPartitions(TaskSyncContext taskSyncContext) {
        return allFilteredPartitionTokens(taskSyncContext, partition -> !inProgressPartitionState(partition.getState())).size();
    }

    public static Set<String> allPartitionTokens(TaskSyncContext taskSyncContext) {
        return allFilteredPartitionTokens(taskSyncContext, partition -> true);
    }

    private static Set<String> allFilteredPartitionTokens(TaskSyncContext taskSyncContext, Predicate<PartitionState> partitionFilter) {
        var allTaskStates = taskSyncContext.getAllTaskStates().values();

        var allOwnedPartitions = allTaskStates.stream()
                .flatMap(t -> t.getPartitions().stream())
                .filter(partitionFilter)
                .map(PartitionState::getToken)
                .collect(toSet());

        var allSharedPartitions = allTaskStates.stream()
                .flatMap(t -> t.getSharedPartitions().stream())
                .filter(p -> !allOwnedPartitions.contains(p))
                .filter(partitionFilter)
                .map(PartitionState::getToken)
                .collect(toSet());

        Set<String> result = new HashSet<>(allOwnedPartitions.size() + allSharedPartitions.size());
        result.addAll(allOwnedPartitions);
        result.addAll(allSharedPartitions);

        return result;
    }

    public static boolean inProgressPartitionState(PartitionStateEnum state) {
        return state != PartitionStateEnum.FINISHED && state != PartitionStateEnum.REMOVED;
    }
}
