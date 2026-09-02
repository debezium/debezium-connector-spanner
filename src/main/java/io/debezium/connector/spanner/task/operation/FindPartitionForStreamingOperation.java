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

import io.debezium.connector.spanner.kafka.internal.model.MoveInState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.task.MoveInGateChecker;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Checks what partitions are ready for streaming.
 *
 * <p>Gate-check logic for MoveIn-paused partitions is delegated to
 * {@link MoveInGateChecker} so that the same implementation is shared by
 * both this state-machine path and the streaming-thread
 * {@link io.debezium.connector.spanner.db.stream.MoveInBufferGate}.
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
        Set<String> finishedPartitions = MoveInGateChecker.getFinishedPartitions(taskSyncContext);

        io.debezium.connector.spanner.kafka.internal.model.TaskState taskState = taskSyncContext.getCurrentTaskState();
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

    /**
     * Determines whether a destination partition that is paused after processing a MoveIn
     * event can resume streaming. This requires that every source partition referenced in the
     * destination's {@link MoveInState} has published a
     * {@link io.debezium.connector.spanner.kafka.internal.model.MoveOutState} that is at or
     * past the MoveIn commit timestamp, and, if exactly at that timestamp, includes this
     * destination partition among its recorded destinations.
     *
     * <p>Delegates gate-check logic to {@link MoveInGateChecker#canContinue} so that the same
     * implementation is shared with the streaming-thread buffer-gate path.
     *
     * <p>Source partitions that have reached {@code FINISHED}/{@code REMOVED} are purged from
     * the task state (see {@link RemoveFinishedPartitionOperation}) and so no longer carry their
     * {@link io.debezium.connector.spanner.kafka.internal.model.MoveOutState}. A source can only
     * reach that state after streaming past every boundary in its key range, including any MoveOut
     * it is a party to, so a source found in {@code finishedPartitions} is treated as having
     * already satisfied this destination's wait condition, rather than deadlocking forever waiting
     * on a purged state.
     *
     * <p>A source can also be missing a matching MoveOutState entry because a task crashed after
     * the source's own change stream query had already read past the MoveIn commit timestamp, but
     * before the resulting {@code MoveOutStateUpdateOperation} update was persisted to the sync
     * topic. If the source's own {@code processedTimestamp} is already strictly past the MoveIn
     * timestamp its change stream has necessarily already read through that boundary for real, so
     * it is safe to treat the MoveOut as satisfied despite the missing local bookkeeping.
     */
    private boolean canDestPartitionContinue(TaskSyncContext taskSyncContext, PartitionState destPartition, Set<String> finishedPartitions) {
        MoveInState moveInState = destPartition.getMoveInState();
        return MoveInGateChecker.canContinue(
                taskSyncContext,
                destPartition.getToken(),
                moveInState.getTimestamp(),
                moveInState.getSourcePartitionTokens(),
                finishedPartitions);
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
