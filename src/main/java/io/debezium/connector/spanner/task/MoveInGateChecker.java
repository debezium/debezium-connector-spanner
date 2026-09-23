/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.PartitionKey;
import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;

/**
 * Shared static gate-check utility for mutable key range move-in ordering. Determines
 * whether a destination partition that is paused after a MoveIn event may resume
 * streaming by verifying that every source partition has published a
 * {@link MoveOutState} at or past the MoveIn commit timestamp.
 *
 * <p>Used by both
 * {@link io.debezium.connector.spanner.task.operation.FindPartitionForStreamingOperation}
 * (state-machine / crash-recovery path) and
 * {@link io.debezium.connector.spanner.db.stream.MoveInBufferGate}
 * (streaming-thread buffer path) so the two paths share exactly one implementation.
 */
public final class MoveInGateChecker {

    private static final Logger LOGGER = LoggerFactory.getLogger(MoveInGateChecker.class);

    private MoveInGateChecker() {
    }

    /**
     * Returns the set of partition identities that are in {@code FINISHED} or {@code REMOVED}
     * state across all task states visible in {@code taskSyncContext}. The identity is the
     * same as {@link PartitionState#getKey()} so callers can scope membership checks by
     * the destination partition's TVF name.
     */
    public static Set<PartitionKey> getFinishedPartitions(TaskSyncContext taskSyncContext) {
        List<PartitionState> all = new ArrayList<>();
        all.addAll(taskSyncContext.getCurrentTaskState().getPartitions());
        taskSyncContext.getTaskStates().values()
                .forEach(ts -> all.addAll(ts.getPartitions()));

        return all.stream()
                .filter(ps -> PartitionStateEnum.FINISHED.equals(ps.getState())
                        || PartitionStateEnum.REMOVED.equals(ps.getState()))
                .map(PartitionState::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Returns {@code true} if every source in {@code sourceTokens} has confirmed its
     * MoveOut at or past {@code moveInTimestamp} for destination {@code destToken}.
     *
     * @param taskSyncContext   live snapshot of the task's known state
     * @param destToken         destination partition token
     * @param destTvfName       destination partition TVF name (may be {@code null} for legacy streams)
     * @param moveInTimestamp   commit timestamp of the MoveIn event
     * @param sourceTokens      all source partition tokens referenced by the MoveIn
     * @param finishedPartitions pre-computed set of identities from {@link #getFinishedPartitions}
     */
    public static boolean canContinue(TaskSyncContext taskSyncContext, String destToken, String destTvfName,
                                      Timestamp moveInTimestamp, List<String> sourceTokens,
                                      Set<PartitionKey> finishedPartitions) {
        for (String sourceToken : sourceTokens) {
            if (!sourceHasResumedThisMove(taskSyncContext, sourceToken, moveInTimestamp, destToken, finishedPartitions, destTvfName)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Mirrors the logic documented on
     * {@code FindPartitionForStreamingOperation#sourceHasResumedThisMove}.
     *
     * @param tvfName the TVF name shared by the source and destination partitions
     */
    public static boolean sourceHasResumedThisMove(TaskSyncContext taskSyncContext,
                                                   String sourceToken,
                                                   Timestamp moveInTimestamp,
                                                   String destToken,
                                                   Set<PartitionKey> finishedPartitions,
                                                   String tvfName) {
        boolean satisfiedByMoveOutState = findMoveOutStates(taskSyncContext, sourceToken, tvfName).stream()
                .anyMatch(mos -> {
                    int cmp = mos.getTimestamp().compareTo(moveInTimestamp);
                    return cmp > 0 || (cmp == 0 && mos.getDestPartitionTokens().contains(destToken));
                });
        if (satisfiedByMoveOutState) {
            return true;
        }
        PartitionKey sourceIdentity = new PartitionKey(sourceToken, tvfName);
        if (finishedPartitions.contains(sourceIdentity)) {
            LOGGER.info("Source partition {} already finished/removed, treating MoveOut as satisfied for destination {}",
                    sourceIdentity, destToken);
            return true;
        }
        PartitionState sourceState = findPartitionState(taskSyncContext, sourceToken, tvfName);
        if (sourceState != null && sourceState.getProcessedTimestamp() != null
                && sourceState.getProcessedTimestamp().compareTo(moveInTimestamp) > 0) {
            LOGGER.info(
                    "Source partition {} already streamed past MoveIn timestamp {} (processedTimestamp={}), "
                            + "treating MoveOut as satisfied for destination {}",
                    sourceIdentity, moveInTimestamp, sourceState.getProcessedTimestamp(), destToken);
            return true;
        }
        return false;
    }

    private static List<MoveOutState> findMoveOutStates(TaskSyncContext taskSyncContext, String token, String tvfName) {
        PartitionState ps = findPartitionState(taskSyncContext, token, tvfName);
        return ps == null ? List.of() : ps.getMoveOutStates();
    }

    /** Searches all task states (partitions and shared partitions) for a matching token and TVF name. */
    public static PartitionState findPartitionState(TaskSyncContext taskSyncContext, String token, String tvfName) {
        for (PartitionState ps : taskSyncContext.getCurrentTaskState().getPartitions()) {
            if (matches(ps, token, tvfName)) {
                return ps;
            }
        }
        for (PartitionState ps : taskSyncContext.getCurrentTaskState().getSharedPartitions()) {
            if (matches(ps, token, tvfName)) {
                return ps;
            }
        }
        for (TaskState ts : taskSyncContext.getTaskStates().values()) {
            for (PartitionState ps : ts.getPartitions()) {
                if (matches(ps, token, tvfName)) {
                    return ps;
                }
            }
            for (PartitionState ps : ts.getSharedPartitions()) {
                if (matches(ps, token, tvfName)) {
                    return ps;
                }
            }
        }
        return null;
    }

    private static boolean matches(PartitionState partition, String token, String tvfName) {
        return partition.getToken().equals(token) && Objects.equals(partition.getTvfName(), tvfName);
    }

}
