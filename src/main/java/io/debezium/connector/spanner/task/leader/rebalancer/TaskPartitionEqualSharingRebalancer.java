/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.leader.rebalancer;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
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
 * Leader distributes partition equally between tasks.
 */
public class TaskPartitionEqualSharingRebalancer implements TaskPartitionRebalancer {
    @Override
    public TaskState rebalance(TaskState leaderTaskState,
                               Map<String, TaskState> survivedTasks,
                               Map<String, TaskState> obsoleteTaskStates) {

        survivedTasks = excludeLeader(leaderTaskState.getTaskUid(), survivedTasks);

        TaskState newLeaderTaskState = moveFinishedPartitionsFromObsoleteTasks(leaderTaskState, obsoleteTaskStates);

        newLeaderTaskState = moveSharedPartitionsFromObsoleteTasks(newLeaderTaskState, survivedTasks, obsoleteTaskStates);

        newLeaderTaskState = takeSharedPartitionsFromSurvivedTasks(newLeaderTaskState, survivedTasks);

        newLeaderTaskState = takeSharedPartitionsToObsoleteTask(newLeaderTaskState, survivedTasks);

        return distributePartitionsFromObsoleteTasks(newLeaderTaskState, survivedTasks, obsoleteTaskStates);
    }

    private Map<String, TaskState> excludeLeader(String leaderTaskUid, Map<String, TaskState> tasks) {
        tasks = new HashMap<>(tasks);
        tasks.remove(leaderTaskUid);
        return tasks;
    }

    private TaskState moveFinishedPartitionsFromObsoleteTasks(TaskState leaderTaskState, Map<String, TaskState> obsoleteTasks) {

        Set<String> tokens = collectPartitionTokens(Set.of(leaderTaskState));

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

    private TaskState distributePartitionsFromObsoleteTasks(TaskState leaderTaskState,
                                                            Map<String, TaskState> survivedTasks,
                                                            Map<String, TaskState> obsoleteTasks) {
        Set<String> tokens = collectPartitionTokens(Set.of(leaderTaskState), survivedTasks.values());

        List<PartitionState> allPartitions = filterDuplications(obsoleteTasks.values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream()).collect(Collectors.toList()));

        List<PartitionState> notFinishedPartitions = allPartitions.stream()
                .filter(partitionState -> !tokens.contains(partitionState.getToken()))
                .map(partitionState -> {
                    if (!PartitionStateEnum.FINISHED.equals(partitionState.getState()) &&
                            !PartitionStateEnum.REMOVED.equals(partitionState.getState())) {
                        return partitionState;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        for (PartitionState partitionState : notFinishedPartitions) {
            List<PartitionState> leaderPartitionList = new ArrayList<>(leaderTaskState.getPartitions());
            List<PartitionState> leaderSharedPartitionList = new ArrayList<>(leaderTaskState.getSharedPartitions());

            String taskUid = findCandidateToSharePartition(leaderTaskState, survivedTasks);

            if (!taskUid.equals(leaderTaskState.getTaskUid())) {
                leaderSharedPartitionList.add(partitionState.toBuilder()
                        .assigneeTaskUid(taskUid)
                        .state(PartitionStateEnum.CREATED)
                        .build());
            }
            else {
                leaderPartitionList.add(partitionState.toBuilder()
                        .assigneeTaskUid(taskUid)
                        .state(PartitionStateEnum.CREATED)
                        .build());
            }

            leaderTaskState = leaderTaskState.toBuilder()
                    .partitions(leaderPartitionList)
                    .sharedPartitions(leaderSharedPartitionList)
                    .build();
        }
        return leaderTaskState;
    }

    private String findCandidateToSharePartition(TaskState leaderTaskState, Map<String, TaskState> survivedTasks) {

        Map<String, TaskState> allTaskStates = new HashMap<>(survivedTasks);
        allTaskStates.put(leaderTaskState.getTaskUid(), leaderTaskState);

        Map<String, Integer> candidateMap = allTaskStates.values().stream().map(taskState -> {
            Set<String> tokens = taskState.getPartitions().stream()
                    .filter(partitionState -> !PartitionStateEnum.FINISHED.equals(partitionState.getState()) &&
                            !PartitionStateEnum.REMOVED.equals(partitionState.getState()))
                    .map(PartitionState::getToken)
                    .collect(Collectors.toCollection(HashSet::new));

            Set<String> assignedTokens = allTaskStates.values().stream().flatMap(taskState1 -> taskState1.getSharedPartitions().stream())
                    .filter(partitionState -> partitionState.getAssigneeTaskUid().equals(taskState.getTaskUid()))
                    .map(PartitionState::getToken)
                    .collect(Collectors.toSet());

            tokens.addAll(assignedTokens);

            return new AbstractMap.SimpleEntry<>(taskState.getTaskUid(), tokens.size());
        }).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        Optional<Integer> minPartitionsValue = candidateMap.values().stream().min(Integer::compare);

        if (minPartitionsValue.isEmpty()) {
            return leaderTaskState.getTaskUid();
        }

        List<String> finalCandidateList = candidateMap.entrySet().stream()
                .filter(entry -> entry.getValue().equals(minPartitionsValue.get()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        int index = new Random().nextInt(finalCandidateList.size());

        return finalCandidateList.get(index);
    }

    private TaskState moveSharedPartitionsFromObsoleteTasks(TaskState leaderTaskState, Map<String, TaskState> survivedTasks,
                                                            Map<String, TaskState> obsoleteTasks) {

        Set<String> tokens = collectPartitionTokens(Set.of(leaderTaskState));
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

        Set<String> tokens = collectPartitionTokens(Set.of(leaderTaskState));

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

    private TaskState takeSharedPartitionsToObsoleteTask(TaskState leaderTaskState, Map<String, TaskState> survivedTasks) {

        Set<String> tokens = leaderTaskState.getPartitionsMap().keySet();

        List<PartitionState> partitions = leaderTaskState.getSharedPartitions()
                .stream()
                .filter(partitionState -> !tokens.contains(partitionState.getToken()))
                .filter(partitionState -> !survivedTasks.containsKey(partitionState.getAssigneeTaskUid()))
                .map(partitionState -> partitionState.toBuilder()
                        .assigneeTaskUid(leaderTaskState.getTaskUid())
                        .build())
                .collect(Collectors.toList());

        List<PartitionState> leaderPartitionList = new ArrayList<>(leaderTaskState.getPartitions());
        leaderPartitionList.addAll(partitions);

        TaskState newLeaderTaskState = leaderTaskState.toBuilder()
                .partitions(leaderPartitionList)
                .build();

        List<PartitionState> leaderSharedPartitionList = leaderTaskState.getSharedPartitions().stream()
                .filter(partitionState -> !newLeaderTaskState.getPartitionsMap().containsKey(partitionState.getToken()))
                .collect(Collectors.toList());

        return newLeaderTaskState.toBuilder()
                .sharedPartitions(leaderSharedPartitionList)
                .build();
    }

    private List<PartitionState> filterDuplications(List<PartitionState> partitionStates) {
        return partitionStates.stream()
                .collect(Collectors.groupingBy(PartitionState::getToken)).values().stream()
                .flatMap(list -> list.stream().sorted().limit(1))
                .collect(Collectors.toList());
    }

    private Set<String> collectPartitionTokens(Collection<TaskState>... taskStates) {
        return Arrays.stream(taskStates).flatMap(Collection::stream).flatMap(taskState -> Stream.concat(
                taskState.getPartitionsMap().keySet().stream(), taskState.getSharedPartitionsMap().keySet().stream()))
                .collect(Collectors.toSet());
    }
}
