/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.ConflictResolver;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * This operation finds out whether new partition will be processing
 * by the current task or shared with other
 */
public class ChildPartitionOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChildPartitionOperation.class);

    private final List<Partition> newPartitions;

    public ChildPartitionOperation(List<Partition> newPartitions) {
        this.newPartitions = newPartitions;
    }

    private TaskSyncContext share(TaskSyncContext taskSyncContext) {

        for (Partition newPartition : newPartitions) {
            TaskState taskState = taskSyncContext.getCurrentTaskState();

            if (!InitialPartition.isInitialPartition(newPartition.getToken())) {
                String priorityParentPartition = ConflictResolver.getPriorityPartition(newPartition.getParentTokens());

                if (!priorityParentPartition.equals(newPartition.getOriginPartitionToken())) {
                    LOGGER.warn("Partition {} ignored. Will be streamed on task with parent partition {}",
                            newPartition.getToken(), priorityParentPartition);
                    continue;
                }
            }

            List<PartitionState> partitions = new ArrayList<>(taskState.getPartitions());
            List<PartitionState> sharedPartitions = new ArrayList<>(taskState.getSharedPartitions());

            if (existPartition(taskSyncContext, newPartition.getToken())) {
                LOGGER.warn("Partition {} already exists in tasks context", newPartition.getToken());
                continue;
            }

            String taskUid = findCandidateToSharePartition(taskSyncContext);

            LOGGER.info("Task {} : share partition {} to {}", taskSyncContext.getTaskUid(), newPartition.getToken(), taskUid);

            PartitionState partitionState = PartitionState.builder()
                    .token(newPartition.getToken())
                    .startTimestamp(newPartition.getStartTimestamp())
                    .endTimestamp(newPartition.getEndTimestamp())
                    .assigneeTaskUid(taskUid)
                    .state(PartitionStateEnum.CREATED)
                    .parents(newPartition.getParentTokens())
                    .originParent(newPartition.getOriginPartitionToken())
                    .build();

            if (taskSyncContext.getTaskUid().equals(taskUid)) {
                partitions.add(partitionState);
                LOGGER.debug("ChildPartitionOperation: added new partition: {}", newPartition.getToken());
            }
            else {
                sharedPartitions.add(partitionState);
                LOGGER.debug("ChildPartitionOperation: shared new partition: {}", newPartition.getToken());
            }

            taskSyncContext = taskSyncContext.toBuilder().currentTaskState(taskState.toBuilder()
                    .partitions(partitions)
                    .sharedPartitions(sharedPartitions)
                    .build()).build();
        }

        return taskSyncContext;
    }

    private boolean existPartition(TaskSyncContext taskSyncContext, String token) {
        boolean found = taskSyncContext.getCurrentTaskState().getPartitions().stream()
                .anyMatch(partition -> token.equals(partition.getToken()));
        if (found) {
            return true;
        }

        found = taskSyncContext.getCurrentTaskState().getSharedPartitions().stream()
                .anyMatch(partition -> token.equals(partition.getToken()));
        if (found) {
            return true;
        }

        found = taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .anyMatch(partition -> token.equals(partition.getToken()));
        if (found) {
            return true;
        }

        found = taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getSharedPartitions().stream())
                .anyMatch(partition -> token.equals(partition.getToken()));

        return found;
    }

    private String findCandidateToSharePartition(TaskSyncContext taskSyncContext) {
        final String currentTaskUid = taskSyncContext.getTaskUid();

        final Collection<TaskState> taskStates = taskSyncContext.getAllTaskStates().values();

        Map<String, Integer> candidateMap = taskStates.stream().map(taskState -> {
            Set<String> tokens = taskState.getPartitions().stream()
                    .filter(partitionState -> !PartitionStateEnum.FINISHED.equals(partitionState.getState()) &&
                            !PartitionStateEnum.REMOVED.equals(partitionState.getState()))
                    .map(PartitionState::getToken)
                    .collect(Collectors.toCollection(HashSet::new));

            Set<String> assignedTokens = taskStates.stream()
                    .flatMap(taskState1 -> taskState1.getSharedPartitions().stream())
                    .filter(partitionState -> partitionState.getAssigneeTaskUid().equals(taskState.getTaskUid()))
                    .map(PartitionState::getToken)
                    .collect(Collectors.toSet());

            tokens.addAll(assignedTokens);

            return new AbstractMap.SimpleEntry<>(taskState.getTaskUid(), tokens.size());
        }).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        Optional<Integer> minPartitionsValue = candidateMap.values().stream().min(Integer::compare);

        if (minPartitionsValue.isEmpty() || minPartitionsValue.get().equals(candidateMap.get(currentTaskUid))) {
            return currentTaskUid;
        }

        List<String> finalCandidateList = candidateMap.entrySet().stream()
                .filter(entry -> entry.getValue().equals(minPartitionsValue.get()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        int index = new Random().nextInt(finalCandidateList.size());

        return finalCandidateList.get(index);
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return true;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        return share(taskSyncContext);
    }

}
