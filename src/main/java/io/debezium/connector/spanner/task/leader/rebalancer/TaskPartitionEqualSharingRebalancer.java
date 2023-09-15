/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.leader.rebalancer;

import static org.slf4j.LoggerFactory.getLogger;

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

import org.slf4j.Logger;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;

/**
 * This task contains the functionality to rebalance change stream partitions from obsolete tasks to
 * survived tasks after a rebalance event.
 *
 * <p>Leader distributes partitions equally between tasks.
 */
public class TaskPartitionEqualSharingRebalancer implements TaskPartitionRebalancer {
    private static final Logger LOGGER = getLogger(TaskPartitionEqualSharingRebalancer.class);

    @Override
    public TaskState rebalance(
                               TaskState leaderTaskState,
                               Map<String, TaskState> survivedTasks,
                               Map<String, TaskState> obsoleteTaskStates) {

        LOGGER.info("Leader task state {}", leaderTaskState.getTaskUid());
        LOGGER.info("Survived tasks {}", survivedTasks.keySet().stream().collect(Collectors.toList()));
        LOGGER.info("Obsolete tasks {}", obsoleteTaskStates.keySet().stream().collect(Collectors.toList()));

        TaskState newLeaderTaskState = moveFinishedPartitionsFromObsoleteTasks(leaderTaskState, obsoleteTaskStates);
        LOGGER.debug(
                "Leader task state after moving finished partitions from obsolete tasks {}",
                newLeaderTaskState);

        newLeaderTaskState = moveSharedPartitionsFromObsoleteTasks(
                newLeaderTaskState, survivedTasks, obsoleteTaskStates);
        LOGGER.debug(
                "Leader task state after moving finished partitions from obsolete tasks {}",
                newLeaderTaskState);

        newLeaderTaskState = takeSharedPartitionsFromSurvivedTasks(newLeaderTaskState, survivedTasks);
        LOGGER.debug(
                "Leader task state after moving shared partitions from survived tasks {}",
                newLeaderTaskState);

        newLeaderTaskState = takeSharedPartitionsToObsoleteTask(newLeaderTaskState, survivedTasks);
        LOGGER.debug(
                "Leader task state after moving shared partitions to obsolete tasks {}",
                newLeaderTaskState);

        newLeaderTaskState = distributePartitionsFromObsoleteTasks(
                newLeaderTaskState, survivedTasks, obsoleteTaskStates);
        LOGGER.debug(
                "Leader task state after distributing partitions from obsolete tasks {}",
                newLeaderTaskState);

        return newLeaderTaskState;
    }

    private TaskState moveFinishedPartitionsFromObsoleteTasks(
                                                              TaskState leaderTaskState, Map<String, TaskState> obsoleteTasks) {

        Set<String> tokens = collectPartitionTokens(Set.of(leaderTaskState));

        List<PartitionState> leaderPartitionList = new ArrayList<>(leaderTaskState.getPartitions());

        List<PartitionState> allPartitions = filterDuplications(
                obsoleteTasks.values().stream()
                        .flatMap(taskState -> taskState.getPartitions().stream())
                        .collect(Collectors.toList()));

        List<PartitionState> finishedPartitions = allPartitions.stream()
                .filter(partitionState -> !tokens.contains(partitionState.getToken()))
                .map(
                        partitionState -> {
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
        LOGGER.info("moveFinishedPartitionsFromObsoleteTasks, Leader task {} now owning finished partition {}", leaderTaskState.getTaskUid(), finishedPartitions);

        return leaderTaskState.toBuilder().partitions(leaderPartitionList).build();
    }

    private TaskState distributePartitionsFromObsoleteTasks(
                                                            TaskState leaderTaskState,
                                                            Map<String, TaskState> survivedTasks,
                                                            Map<String, TaskState> obsoleteTasks) {
        // Get a list of all partitions owned or shared by all survived tasks, including leader
        Set<String> tokens = collectPartitionTokens(Set.of(leaderTaskState), survivedTasks.values());

        // Get a list of all partitions owned by the obsolete tasks.
        List<PartitionState> allPartitions = filterDuplications(
                obsoleteTasks.values().stream()
                        .flatMap(taskState -> taskState.getPartitions().stream())
                        .collect(Collectors.toList()));

        // Filter the above list to include non finished or non removed partitions.
        List<PartitionState> notFinishedPartitions = allPartitions.stream()
                .filter(partitionState -> !tokens.contains(partitionState.getToken()))
                .map(
                        partitionState -> {
                            if (!PartitionStateEnum.FINISHED.equals(partitionState.getState())
                                    && !PartitionStateEnum.REMOVED.equals(partitionState.getState())) {
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
                leaderSharedPartitionList.add(
                        partitionState.toBuilder()
                                .assigneeTaskUid(taskUid)
                                .state(PartitionStateEnum.CREATED)
                                .build());
                LOGGER.info("distributePartitionsFromObsoleteTasks, Leader task {} now sharing partition {} to {}", leaderTaskState.getTaskUid(),
                        partitionState.getToken(), taskUid);
            }
            else {
                leaderPartitionList.add(
                        partitionState.toBuilder()
                                .assigneeTaskUid(taskUid)
                                .state(PartitionStateEnum.CREATED)
                                .build());
                LOGGER.info("distributePartitionsFromObsoleteTasks, Leader task {} now owning partition {}", leaderTaskState.getTaskUid(), partitionState.getToken());
            }

            leaderTaskState = leaderTaskState.toBuilder()
                    .partitions(leaderPartitionList)
                    .sharedPartitions(leaderSharedPartitionList)
                    .build();
        }
        return leaderTaskState;
    }

    private String findCandidateToSharePartition(
                                                 TaskState leaderTaskState, Map<String, TaskState> survivedTasks) {

        Map<String, TaskState> allTaskStates = new HashMap<>(survivedTasks);
        allTaskStates.put(leaderTaskState.getTaskUid(), leaderTaskState);

        Map<String, Integer> candidateMap = allTaskStates.values().stream()
                .map(
                        taskState -> {
                            Set<String> tokens = taskState.getPartitions().stream()
                                    .filter(
                                            partitionState -> !PartitionStateEnum.FINISHED.equals(partitionState.getState())
                                                    && !PartitionStateEnum.REMOVED.equals(
                                                            partitionState.getState()))
                                    .map(PartitionState::getToken)
                                    .collect(Collectors.toCollection(HashSet::new));

                            Set<String> assignedTokens = allTaskStates.values().stream()
                                    .flatMap(taskState1 -> taskState1.getSharedPartitions().stream())
                                    .filter(
                                            partitionState -> partitionState
                                                    .getAssigneeTaskUid()
                                                    .equals(taskState.getTaskUid()))
                                    .map(PartitionState::getToken)
                                    .collect(Collectors.toSet());

                            tokens.addAll(assignedTokens);

                            return new AbstractMap.SimpleEntry<>(taskState.getTaskUid(), tokens.size());
                        })
                .collect(
                        Collectors.toMap(
                                AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

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

    private TaskState moveSharedPartitionsFromObsoleteTasks(
                                                            TaskState leaderTaskState,
                                                            Map<String, TaskState> survivedTasks,
                                                            Map<String, TaskState> obsoleteTasks) {

        // Collect owned and shared partition tokens from the leader task state.
        Set<String> leaderTokens = collectPartitionTokens(Set.of(leaderTaskState));

        // Collect a list of all tokens owned by survived tasks not including leader.
        Set<String> survivedOwnedTokens = collectOwnedPartitionTokens(survivedTasks.values());
        String leaderUid = leaderTaskState.getTaskUid();

        // Get a list of the shared partitions from the obsolete tasks.
        List<PartitionState> obsoleteTasksSharedPartitions = filterDuplications(
                obsoleteTasks.values().stream()
                        .flatMap(taskState -> taskState.getSharedPartitions().stream())
                        .collect(Collectors.toList()));

        List<PartitionState> leaderSharedPartitionList = new ArrayList<>(leaderTaskState.getSharedPartitions());

        List<PartitionState> leaderPartitionList = new ArrayList<>(leaderTaskState.getPartitions());

        // 1. We get the shared partitions from the obsolete tasks that are assigned to the survived tasks,
        // not including the leader.
        // These tokens also should not be shared by the leader or owned by the leader or owned by the
        // survived tasks.
        List<PartitionState> newSharedPartitions = obsoleteTasksSharedPartitions.stream()
                .filter(partitionState -> !leaderTokens.contains(partitionState.getToken()))
                .filter(partitionState -> !survivedOwnedTokens.contains(partitionState.getToken()))
                .map(
                        partitionState -> {
                            if (survivedTasks.containsKey(partitionState.getAssigneeTaskUid())) {
                                return partitionState;
                            }
                            return null;
                        })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        LOGGER.info("moveSharedPartitionsFromObsoleteTasks, Leader task {} now sharing partitions {}", leaderTaskState.getTaskUid(), newSharedPartitions);
        leaderSharedPartitionList.addAll(newSharedPartitions);

        // 2. We get the shared partitions from the obsolete tasks that are assigned to the leader task
        // These tokens also should not be shared by the leader or owned by the leader or owned by the
        // survived tasks.
        List<PartitionState> newOwnedPartitions = obsoleteTasksSharedPartitions.stream()
                .filter(partitionState -> !leaderTokens.contains(partitionState.getToken()))
                .filter(partitionState -> !survivedOwnedTokens.contains(partitionState.getToken()))
                .map(
                        partitionState -> {
                            if (leaderUid.equals(partitionState.getAssigneeTaskUid())) {
                                return partitionState;
                            }
                            return null;
                        })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        leaderPartitionList.addAll(newOwnedPartitions);
        LOGGER.info("moveSharedPartitionsFromObsoleteTasks, Leader task {} now owning partitions {}", leaderTaskState.getTaskUid(), newOwnedPartitions);

        leaderTaskState = leaderTaskState.toBuilder()
                .partitions(leaderPartitionList)
                .sharedPartitions(leaderSharedPartitionList)
                .build();

        // Get the tokens shared to survived tasks, and re-collect the tokens owned / shared by the leader.
        Set<String> tokensSharedToSurvivedTasks = collectTokensSharedToSurvivedTasks(leaderTaskState, survivedTasks.values());
        Set<String> newLeaderTokens = collectPartitionTokens(Set.of(leaderTaskState));

        // Finally, we get the partitions shared by the obsolete tasks that are:
        // 1. not owned or shared by the leader.
        // 2. not owned by a survived task, not including the leader.
        // 3. not shared to a survived task or leader currently.
        // These partitions should not be owned by the leader or the survived tasks.
        List<PartitionState> newPartitions = obsoleteTasksSharedPartitions.stream()
                .filter(partitionState -> !newLeaderTokens.contains(partitionState.getToken()))
                .filter(partitionState -> !survivedOwnedTokens.contains(partitionState.getToken()))
                .filter(partitionState -> !tokensSharedToSurvivedTasks.contains(partitionState.getToken()))
                .collect(Collectors.toList());
        for (PartitionState partitionState : newPartitions) {
            String taskUid = findCandidateToSharePartition(leaderTaskState, survivedTasks);

            if (!taskUid.equals(leaderTaskState.getTaskUid())) {
                leaderSharedPartitionList.add(
                        partitionState.toBuilder()
                                .assigneeTaskUid(taskUid)
                                .state(PartitionStateEnum.CREATED)
                                .build());
                LOGGER.info("moveSharedPartitionsFromObsoleteTasks, Leader task {} now sharing partition {} to {}", leaderTaskState.getTaskUid(),
                        partitionState.getToken(), taskUid);
            }
            else {
                leaderPartitionList.add(
                        partitionState.toBuilder()
                                .assigneeTaskUid(taskUid)
                                .state(PartitionStateEnum.CREATED)
                                .build());
                LOGGER.info("moveSharedPartitionsFromObsoleteTasks, Leader task {} now owning partition {}", leaderTaskState.getTaskUid(), partitionState.getToken());
            }

            leaderTaskState = leaderTaskState.toBuilder()
                    .partitions(leaderPartitionList)
                    .sharedPartitions(leaderSharedPartitionList)
                    .build();
        }

        return leaderTaskState;
    }

    private TaskState takeSharedPartitionsFromSurvivedTasks(
                                                            TaskState leaderTaskState, Map<String, TaskState> survivedTasks) {

        Set<String> tokens = collectPartitionTokens(Set.of(leaderTaskState));
        Set<String> survivedOwnedTokens = collectOwnedPartitionTokens(survivedTasks.values());
        Set<String> tokensSharedToSurvivedTasks = collectTokensSharedToSurvivedTasks(leaderTaskState, survivedTasks.values());
        String leaderUid = leaderTaskState.getTaskUid();

        // You get a list of partition tokens shared by the survived tasks that are not:
        // 1. owned or shared by the leader.
        // 2. not owned by a survived task, not including the leader.a
        // 3. not shared to a survived task, including the leader.

        List<PartitionState> partitions = filterDuplications(
                survivedTasks.values().stream()
                        .flatMap(taskState -> taskState.getSharedPartitions().stream())
                        .collect(Collectors.toList()))
                .stream()
                .filter(partitionState -> !tokens.contains(partitionState.getToken()))
                .filter(partitionState -> !survivedOwnedTokens.contains(partitionState.getToken()))
                .filter(
                        partitionState -> !tokensSharedToSurvivedTasks.contains(partitionState.getToken()))
                .collect(Collectors.toList());

        List<PartitionState> leaderPartitionList = new ArrayList<>(leaderTaskState.getPartitions());
        List<PartitionState> leaderSharedPartitionList = new ArrayList<>(leaderTaskState.getSharedPartitions());
        for (PartitionState partitionState : partitions) {
            String taskUid = findCandidateToSharePartition(leaderTaskState, survivedTasks);

            // If it is assigned to the leader, we put it in the leader's own list.
            // These partitions will still be in the survived task's share list for a while, which means
            // if another rebalance happens, it could be shared
            // again.
            if (!taskUid.equals(leaderUid)) {
                leaderSharedPartitionList.add(
                        partitionState.toBuilder()
                                .assigneeTaskUid(taskUid)
                                .state(PartitionStateEnum.CREATED)
                                .build());
                LOGGER.info("takeSharedPartitionsFromSurvivedTasks, Leader task {} now sharing partition {} to {}", leaderTaskState.getTaskUid(),
                        partitionState.getToken(), taskUid);
            }
            else {
                // If not assigned to the leader, we put it in the leader share list.
                leaderPartitionList.add(
                        partitionState.toBuilder()
                                .assigneeTaskUid(taskUid)
                                .state(PartitionStateEnum.CREATED)
                                .build());
                LOGGER.info("takeSharedPartitionsFromSurvivedTasks, Leader task {} now owning partition {}", leaderTaskState.getTaskUid(), partitionState.getToken());
            }

            leaderTaskState = leaderTaskState.toBuilder()
                    .partitions(leaderPartitionList)
                    .sharedPartitions(leaderSharedPartitionList)
                    .build();
        }

        return leaderTaskState;
    }

    private TaskState takeSharedPartitionsToObsoleteTask(
                                                         TaskState leaderTaskState, Map<String, TaskState> survivedTasks) {

        Set<String> tokens = leaderTaskState.getPartitionsMap().keySet();
        Set<String> survivedOwnedTokens = collectOwnedPartitionTokens(survivedTasks.values());
        Set<String> tokensSharedToSurvivedTasks = collectTokensSharedToSurvivedTasks(leaderTaskState, survivedTasks.values());

        // The leader itself has shared a token to an obsolete task. The token is not owned by the
        // leader, not owned by any of the survived tasks, nor is it shared to a survived task.
        List<PartitionState> partitions = leaderTaskState.getSharedPartitions().stream()
                .filter(partitionState -> !tokens.contains(partitionState.getToken()))
                .filter(partitionState -> !survivedOwnedTokens.contains(partitionState.getToken()))
                .filter(
                        partitionState -> !tokensSharedToSurvivedTasks.contains(partitionState.getToken()))
                .map(
                        partitionState -> partitionState.toBuilder()
                                .assigneeTaskUid(leaderTaskState.getTaskUid())
                                .build())
                .collect(Collectors.toList());

        List<PartitionState> leaderPartitionList = new ArrayList<>(leaderTaskState.getPartitions());
        leaderPartitionList.addAll(partitions);
        LOGGER.info("takeSharedPartitionsToObsoleteTask, Leader task {} now owning partitions {}", leaderTaskState.getTaskUid(), partitions);

        TaskState newLeaderTaskState = leaderTaskState.toBuilder().partitions(leaderPartitionList).build();

        List<PartitionState> leaderSharedPartitionList = leaderTaskState.getSharedPartitions().stream()
                .filter(
                        partitionState -> !newLeaderTaskState.getPartitionsMap().containsKey(partitionState.getToken()))
                .collect(Collectors.toList());

        return newLeaderTaskState.toBuilder().sharedPartitions(leaderSharedPartitionList).build();
    }

    private List<PartitionState> filterDuplications(List<PartitionState> partitionStates) {
        return partitionStates.stream()
                .collect(Collectors.groupingBy(PartitionState::getToken))
                .values()
                .stream()
                .flatMap(list -> list.stream().sorted().limit(1))
                .collect(Collectors.toList());
    }

    // Collect the tokens owned and shared by the set of tasks.
    private Set<String> collectPartitionTokens(Collection<TaskState>... taskStates) {
        return Arrays.stream(taskStates)
                .flatMap(Collection::stream)
                .flatMap(
                        taskState -> Stream.concat(
                                taskState.getPartitionsMap().keySet().stream(),
                                taskState.getSharedPartitionsMap().keySet().stream()))
                .collect(Collectors.toSet());
    }

    // Collect the partitions owned by the set of tasks.
    private Set<String> collectOwnedPartitionTokens(Collection<TaskState>... taskStates) {
        return Arrays.stream(taskStates)
                .flatMap(Collection::stream)
                .flatMap(taskState -> taskState.getPartitionsMap().keySet().stream())
                .collect(Collectors.toSet());
    }

    // Collect all tokens shared to all survived tasks, including leader.
    private Set<String> collectTokensSharedToSurvivedTasks(TaskState leaderTaskState, Collection<TaskState>... survivedTasks) {
        // Get the list of total survived tasks, including the leader.
        Set<TaskState> allSurvivedTasks = new HashSet<>();
        allSurvivedTasks.addAll(Arrays.stream(survivedTasks).flatMap(Collection::stream).collect(Collectors.toSet()));
        allSurvivedTasks.add(leaderTaskState);
        Set<String> allSurvivedTaskUids = allSurvivedTasks.stream().map(taskState -> taskState.getTaskUid())
                .collect(Collectors.toSet());
        // For each of the survived tasks, including the leader, get the shared partitions that have been assigned to an alive task.
        return allSurvivedTasks.stream()
                .flatMap(taskState -> taskState.getSharedPartitionsMap().values().stream())
                .filter(partition -> allSurvivedTaskUids.contains(partition.getAssigneeTaskUid()))
                .map(partition -> partition.getToken())
                .collect(Collectors.toSet());
    }
}
