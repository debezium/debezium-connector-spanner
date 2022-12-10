/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.leader.rebalancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;

/**
 * This task contains the functionality to rebalance change stream partitions from obsolete tasks
 * to survived tasks after a rebalance event.
 *
 * Leader takes everything.
 */
public class TaskPartitionGreedyLeaderRebalancer implements TaskPartitionRebalancer {
    @Override
    public TaskState rebalance(TaskState leaderTaskState,
                               Map<String, TaskState> survivedTasks,
                               Map<String, TaskState> obsoleteTaskStates) {

        survivedTasks = excludeLeader(leaderTaskState.getTaskUid(), survivedTasks);

        TaskState newLeaderTaskState = moveFinishedPartitionsFromObsoleteTasks(leaderTaskState, obsoleteTaskStates);

        newLeaderTaskState = movePartitionsFromObsoleteTasks(newLeaderTaskState, obsoleteTaskStates);

        newLeaderTaskState = moveSharedPartitionsFromObsoleteTasks(newLeaderTaskState, survivedTasks, obsoleteTaskStates);

        return takeSharedPartitionsFromSurvivedTasks(newLeaderTaskState, survivedTasks);
    }

    private Map<String, TaskState> excludeLeader(String leaderTaskUid, Map<String, TaskState> tasks) {
        tasks = new HashMap<>(tasks);
        tasks.remove(leaderTaskUid);
        return tasks;
    }

    private TaskState movePartitionsFromObsoleteTasks(TaskState leaderTaskState, Map<String, TaskState> obsoleteTasks) {

        Set<String> tokens = collectPartitionTokens(leaderTaskState);

        List<PartitionState> leaderPartitionList = new ArrayList<>(leaderTaskState.getPartitions());

        List<PartitionState> allPartitions = filterDuplications(obsoleteTasks.values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream()).collect(Collectors.toList()));

        List<PartitionState> partitions = allPartitions.stream().filter(partitionState -> !tokens.contains(partitionState.getToken()))
                .map(partitionState -> {
                    if (PartitionStateEnum.SCHEDULED.equals(partitionState.getState()) ||
                            PartitionStateEnum.RUNNING.equals(partitionState.getState())) {
                        return partitionState.toBuilder()
                                .state(PartitionStateEnum.READY_FOR_STREAMING)
                                .assigneeTaskUid(leaderTaskState.getTaskUid())
                                .build();
                    }
                    return partitionState.toBuilder()
                            .assigneeTaskUid(leaderTaskState.getTaskUid())
                            .build();
                })
                .collect(Collectors.toList());
        leaderPartitionList.addAll(partitions);

        return leaderTaskState.toBuilder()
                .partitions(leaderPartitionList)
                .build();
    }

    private TaskState moveSharedPartitionsFromObsoleteTasks(TaskState leaderTaskState, Map<String, TaskState> survivedTasks,
                                                            Map<String, TaskState> obsoleteTasks) {

        Set<String> tokens = collectPartitionTokens(leaderTaskState);
        String leaderUid = leaderTaskState.getTaskUid();

        List<PartitionState> obsoleteTasksSharedPartitions = filterDuplications(obsoleteTasks.values().stream()
                .flatMap(taskState -> taskState.getSharedPartitions().stream()).collect(Collectors.toList()));

        List<PartitionState> leaderSharedPartitionList = new ArrayList<>(leaderTaskState.getSharedPartitions());

        List<PartitionState> leaderPartitionList = new ArrayList<>(leaderTaskState.getPartitions());

        List<PartitionState> newSharedPartitions = obsoleteTasksSharedPartitions.stream()
                .filter(partitionState -> !tokens.contains(partitionState.getToken()))
                .map(partitionState -> {
                    if (survivedTasks.containsKey(partitionState.getAssigneeTaskUid())
                            && !partitionState.getAssigneeTaskUid().equals(leaderUid)) {
                        return partitionState;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        leaderSharedPartitionList.addAll(newSharedPartitions);

        List<PartitionState> newPartitions = obsoleteTasksSharedPartitions.stream()
                .filter(partitionState -> !tokens.contains(partitionState.getToken()))
                .map(partitionState -> {
                    if (!survivedTasks.containsKey(partitionState.getAssigneeTaskUid())
                            || partitionState.getAssigneeTaskUid().equals(leaderUid)) {
                        return partitionState.toBuilder()
                                .assigneeTaskUid(leaderTaskState.getTaskUid())
                                .build();
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        leaderPartitionList.addAll(newPartitions);

        return leaderTaskState.toBuilder()
                .partitions(leaderPartitionList)
                .sharedPartitions(leaderSharedPartitionList)
                .build();
    }

    private TaskState takeSharedPartitionsFromSurvivedTasks(TaskState leaderTaskState,
                                                            Map<String, TaskState> survivedTasks) {

        Set<String> tokens = collectPartitionTokens(leaderTaskState);

        List<PartitionState> partitions = filterDuplications(survivedTasks.values().stream()
                .flatMap(taskState -> taskState.getSharedPartitions().stream()).collect(Collectors.toList()))
                        .stream()
                        .filter(partitionState -> !tokens.contains(partitionState.getToken()))
                        .filter(partitionState -> !survivedTasks.containsKey(partitionState.getAssigneeTaskUid()))
                        .map(partitionState -> partitionState.toBuilder()
                                .assigneeTaskUid(leaderTaskState.getTaskUid())
                                .build())
                        .collect(Collectors.toList());

        List<PartitionState> leaderPartitionList = new ArrayList<>(leaderTaskState.getPartitions());
        leaderPartitionList.addAll(partitions);

        return leaderTaskState.toBuilder()
                .partitions(leaderPartitionList)
                .build();
    }

    private TaskState moveFinishedPartitionsFromObsoleteTasks(TaskState leaderTaskState, Map<String, TaskState> obsoleteTasks) {

        Set<String> tokens = collectPartitionTokens(leaderTaskState);

        List<PartitionState> leaderPartitionList = new ArrayList<>(leaderTaskState.getPartitions());

        List<PartitionState> allPartitions = filterDuplications(obsoleteTasks.values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream()).collect(Collectors.toList()));

        List<PartitionState> finishedPartitions = allPartitions.stream()
                .filter(partitionState -> !tokens.contains(partitionState.getToken()))
                .map(partitionState -> {
                    if (PartitionStateEnum.FINISHED.equals(partitionState.getState())) {
                        return partitionState.toBuilder()
                                .assigneeTaskUid(leaderTaskState.getTaskUid())
                                .build();
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        leaderPartitionList.addAll(finishedPartitions);

        return leaderTaskState.toBuilder()
                .partitions(leaderPartitionList)
                .build();
    }

    private List<PartitionState> filterDuplications(List<PartitionState> partitionStates) {
        return partitionStates.stream().collect(Collectors.groupingBy(PartitionState::getToken)).values().stream()
                .flatMap(list -> list.stream().sorted().limit(1)).collect(Collectors.toList());
    }

    private Set<String> collectPartitionTokens(TaskState taskState) {
        return Stream.concat(
                taskState.getPartitionsMap().keySet().stream(),
                taskState.getSharedPartitionsMap().keySet().stream())
                .collect(Collectors.toSet());
    }
}
