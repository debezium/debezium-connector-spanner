/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.MoveInState;
import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Checks what partitions are ready for streaming
 */
public class FindPartitionForStreamingOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionForStreamingOperation.class);

    private boolean isRequiredPublishSyncEvent = false;
    private final boolean isMutableKeyRange;

    public FindPartitionForStreamingOperation() {
        this(false);
    }

    public FindPartitionForStreamingOperation(boolean isMutableKeyRange) {
        this.isMutableKeyRange = isMutableKeyRange;
    }

    private TaskSyncContext takePartitionForStreaming(TaskSyncContext taskSyncContext) {
        Set<String> finishedPartitions = getFinishedPartitions(taskSyncContext);

        TaskState taskState = taskSyncContext.getCurrentTaskState();
        List<PartitionState> partitions = taskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getState().equals(PartitionStateEnum.CREATED)) {
                        boolean takePartitionForStreaming = false;
                        LOGGER.debug("Task sees partition with CREATED state, task Uid {}, partition {}", taskSyncContext.getTaskUid(), partitionState);
                        if (partitionState.getMoveInState() != null) {
                            if (canDestPartitionContinue(taskSyncContext, partitionState, finishedPartitions)) {
                                LOGGER.info("Task takes MoveIn partition for streaming, source(s) processed MoveOut, taskUid: {}, partition {}",
                                        taskSyncContext.getTaskUid(), partitionState.getToken());
                                takePartitionForStreaming = true;
                            }
                            else {
                                LOGGER.info("Task not taking MoveIn partition for streaming, waiting for source(s) MoveOut, taskUid: {}, partition {}, sources {}",
                                        taskSyncContext.getTaskUid(), partitionState.getToken(), partitionState.getParents());
                            }
                        }
                        else if (finishedPartitions.containsAll(partitionState.getParents()) || isMutableKeyRange) {
                            takePartitionForStreaming = true;
                            LOGGER.info("Task takes partition for streaming, taskUid: {}, partition {}",
                                    taskSyncContext.getTaskUid(), partitionState.getToken());

                        }
                        else if (!atLeastOneParentExists(taskSyncContext, partitionState.getParents())) {
                            LOGGER.info("Task takes partition for streaming, since parents no longer exist, taskUid: {}, partition {}, parents {}",
                                    taskSyncContext.getTaskUid(), partitionState.getToken(), partitionState.getParents());
                            takePartitionForStreaming = true;
                        }
                        else {
                            LOGGER.info("Task not taking partition for streaming, since parents are not finished, taskUid: {}, partition {}, parents {}",
                                    taskSyncContext.getTaskUid(), partitionState.getToken(), partitionState.getParents());

                        }

                        if (takePartitionForStreaming) {
                            this.isRequiredPublishSyncEvent = true;

                            return partitionState.toBuilder()
                                    .state(PartitionStateEnum.READY_FOR_STREAMING)
                                    .build();
                        }
                        else {
                            return partitionState;
                        }
                    }
                    return partitionState;
                }).collect(Collectors.toList());

        return taskSyncContext.toBuilder()
                .currentTaskState(taskState.toBuilder().partitions(partitions).build())
                .build();
    }

    private Set<String> getFinishedPartitions(TaskSyncContext taskSyncContext) {
        List<PartitionState> partitionStateList = new ArrayList<>();
        partitionStateList.addAll(taskSyncContext.getCurrentTaskState().getPartitions());
        partitionStateList.addAll(taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .collect(Collectors.toList()));

        return partitionStateList.stream()
                .filter(partitionState -> PartitionStateEnum.FINISHED.equals(partitionState.getState())
                        || PartitionStateEnum.REMOVED.equals(partitionState.getState()))
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());
    }

    /**
     * Determines whether a destination partition that is paused after processing a MoveIn
     * event can resume streaming. This requires that every source partition referenced in the
     * destination's {@link MoveInState} has published a {@link MoveOutState} that is at or past
     * the MoveIn commit timestamp, and, if exactly at that timestamp, includes this destination
     * partition among its recorded destinations.
     *
     * <p>Source partitions that have reached {@code FINISHED}/{@code REMOVED} are purged from
     * the task state (see {@link RemoveFinishedPartitionOperation}) and so no longer carry their
     * {@link MoveOutState}. A source can only reach that state after streaming past every
     * boundary in its key range, including any MoveOut it is a party to, so a source found in
     * {@code finishedPartitions} is treated as having already satisfied this destination's wait
     * condition, rather than deadlocking forever waiting on a purged {@link MoveOutState}.
     *
     * <p>A source can also be missing a matching {@link MoveOutState} entry because a task
     * crashed after the source's own change stream query had already read past the MoveIn commit
     * timestamp, but before the resulting {@code MoveOutStateUpdateOperation} update was
     * persisted to the sync topic. On restart, the source resumes from its persisted offset -
     * which is already past that timestamp - so it will never re-read (and therefore never
     * re-emit) that boundary record again. If the source's own {@code processedTimestamp} is
     * already strictly past the MoveIn timestamp, its change stream has necessarily already read
     * through that boundary for real (Spanner change streams deliver records in
     * commit-timestamp order), so it is safe to treat the MoveOut as satisfied despite the
     * missing local bookkeeping. This fallback is checked whenever no entry proves the move is
     * satisfied - not only when {@code moveOutStates} is completely empty - since a source can
     * accumulate several independent MoveOut entries over its life (see
     * {@code MoveOutStateUpdateOperation}) and an older, unrelated entry must never mask the loss
     * of a different, later one.
     */
    private boolean canDestPartitionContinue(TaskSyncContext taskSyncContext, PartitionState destPartition, Set<String> finishedPartitions) {
        MoveInState moveInState = destPartition.getMoveInState();
        Timestamp moveInTimestamp = moveInState.getTimestamp();
        String destToken = destPartition.getToken();

        for (String sourceToken : moveInState.getSourcePartitionTokens()) {
            if (!sourceHasResumedThisMove(taskSyncContext, sourceToken, moveInTimestamp, destToken, finishedPartitions)) {
                return false;
            }
        }
        return true;
    }

    private boolean sourceHasResumedThisMove(TaskSyncContext taskSyncContext, String sourceToken, Timestamp moveInTimestamp,
                                             String destToken, Set<String> finishedPartitions) {
        boolean satisfiedByMoveOutState = findMoveOutStates(taskSyncContext, sourceToken).stream()
                .anyMatch(moveOutState -> {
                    int cmp = moveOutState.getTimestamp().compareTo(moveInTimestamp);
                    return cmp > 0 || (cmp == 0 && moveOutState.getDestPartitionTokens().contains(destToken));
                });
        if (satisfiedByMoveOutState) {
            return true;
        }
        if (finishedPartitions.contains(sourceToken)) {
            LOGGER.info("Task {}, source partition {} already finished and purged, treating MoveOut as satisfied for destination {}",
                    taskSyncContext.getTaskUid(), sourceToken, destToken);
            return true;
        }
        PartitionState sourceState = findPartitionState(taskSyncContext, sourceToken);
        if (sourceState != null && sourceState.getProcessedTimestamp() != null
                && sourceState.getProcessedTimestamp().compareTo(moveInTimestamp) > 0) {
            LOGGER.info(
                    "Task {}, source partition {} already streamed past MoveIn timestamp {} (processedTimestamp={}) despite missing a matching MoveOutState "
                            + "(likely lost in a crash before it was persisted), treating MoveOut as satisfied for destination {}",
                    taskSyncContext.getTaskUid(), sourceToken, moveInTimestamp, sourceState.getProcessedTimestamp(), destToken);
            return true;
        }
        return false;
    }

    private List<MoveOutState> findMoveOutStates(TaskSyncContext taskSyncContext, String token) {
        PartitionState partitionState = findPartitionState(taskSyncContext, token);
        return partitionState == null ? List.of() : partitionState.getMoveOutStates();
    }

    private PartitionState findPartitionState(TaskSyncContext taskSyncContext, String token) {
        for (PartitionState partitionState : taskSyncContext.getCurrentTaskState().getPartitions()) {
            if (partitionState.getToken().equals(token)) {
                return partitionState;
            }
        }
        for (PartitionState partitionState : taskSyncContext.getCurrentTaskState().getSharedPartitions()) {
            if (partitionState.getToken().equals(token)) {
                return partitionState;
            }
        }
        for (TaskState taskState : taskSyncContext.getTaskStates().values()) {
            for (PartitionState partitionState : taskState.getPartitions()) {
                if (partitionState.getToken().equals(token)) {
                    return partitionState;
                }
            }
            for (PartitionState partitionState : taskState.getSharedPartitions()) {
                if (partitionState.getToken().equals(token)) {
                    return partitionState;
                }
            }
        }
        return null;
    }

    private boolean atLeastOneParentExists(TaskSyncContext taskSyncContext, Set<String> parents) {
        List<PartitionState> partitionStateList = new ArrayList<>();
        partitionStateList.addAll(taskSyncContext.getCurrentTaskState().getPartitions());
        partitionStateList.addAll(taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .collect(Collectors.toList()));
        partitionStateList.addAll(taskSyncContext.getCurrentTaskState().getSharedPartitions());
        partitionStateList.addAll(taskSyncContext.getTaskStates().values().stream()
                .flatMap(taskState -> taskState.getSharedPartitions().stream())
                .collect(Collectors.toList()));

        Set<String> allPartitions = partitionStateList.stream()
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());
        for (String parent : parents) {
            if (allPartitions.contains(parent)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return isRequiredPublishSyncEvent;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        return takePartitionForStreaming(taskSyncContext);
    }
}
