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

import io.debezium.connector.spanner.db.model.InitialPartition;
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

    /**
     * Callback used when a MoveIn source partition token is absent from {@link TaskSyncContext}
     * across all tracked TVFs to probe whether the token belongs to an external placement not
     * managed by this connector instance.
     */
    @FunctionalInterface
    public interface PlacementTokenProbe {
        boolean isExternalPlacementToken(String partitionToken, Timestamp probeTimestamp);
    }

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
     */
    public static boolean canContinue(TaskSyncContext taskSyncContext, String destToken, String destTvfName,
                                      Timestamp moveInTimestamp, List<String> sourceTokens,
                                      Set<PartitionKey> finishedPartitions) {
        return canContinue(taskSyncContext, destToken, destTvfName, moveInTimestamp, sourceTokens, finishedPartitions, null);
    }

    /**
     * Returns {@code true} if every source in {@code sourceTokens} has confirmed its
     * MoveOut at or past {@code moveInTimestamp} for destination {@code destToken}, or belongs
     * to an external placement outside this connector's configured placement TVFs.
     *
     * @param taskSyncContext     live snapshot of the task's known state
     * @param destToken           destination partition token
     * @param destTvfName         destination partition TVF name (may be {@code null} for legacy streams)
     * @param moveInTimestamp     commit timestamp of the MoveIn event
     * @param sourceTokens        all source partition tokens referenced by the MoveIn
     * @param finishedPartitions  pre-computed set of identities from {@link #getFinishedPartitions}
     * @param placementTokenProbe optional probe for external placement tokens when absent from {@code taskSyncContext}
     */
    public static boolean canContinue(TaskSyncContext taskSyncContext, String destToken, String destTvfName,
                                      Timestamp moveInTimestamp, List<String> sourceTokens,
                                      Set<PartitionKey> finishedPartitions,
                                      PlacementTokenProbe placementTokenProbe) {
        for (String sourceToken : sourceTokens) {
            if (!sourceHasResumedThisMove(taskSyncContext, sourceToken, moveInTimestamp, destToken,
                    finishedPartitions, destTvfName, placementTokenProbe)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Mirrors the logic documented on
     * {@code FindPartitionForStreamingOperation#sourceHasResumedThisMove}.
     *
     * @param tvfName the TVF name of the destination partition
     */
    public static boolean sourceHasResumedThisMove(TaskSyncContext taskSyncContext,
                                                   String sourceToken,
                                                   Timestamp moveInTimestamp,
                                                   String destToken,
                                                   Set<PartitionKey> finishedPartitions,
                                                   String tvfName) {
        return sourceHasResumedThisMove(taskSyncContext, sourceToken, moveInTimestamp, destToken,
                finishedPartitions, tvfName, null);
    }

    /**
     * Evaluates whether {@code sourceToken} has satisfied the MoveOut requirement for
     * {@code destToken} at {@code moveInTimestamp}:
     * <ol>
     *   <li>First checks for {@code (sourceToken, tvfName)} in the same TVF.</li>
     *   <li>If not found in {@code tvfName} and {@code tvfName != null} (per-placement TVF mode),
     *       searches {@code taskSyncContext} and {@code finishedPartitions} across any co-located
     *       TVF so cross-placement moves between co-located TVFs preserve ordering with 0 RPCs.</li>
     *   <li>If {@code sourceToken} is absent from {@code taskSyncContext} across all TVFs, invokes
     *       {@code placementTokenProbe} (if non-null) to check whether {@code sourceToken} belongs
     *       to an external placement not tracked by this connector.</li>
     * </ol>
     */
    public static boolean sourceHasResumedThisMove(TaskSyncContext taskSyncContext,
                                                   String sourceToken,
                                                   Timestamp moveInTimestamp,
                                                   String destToken,
                                                   Set<PartitionKey> finishedPartitions,
                                                   String tvfName,
                                                   PlacementTokenProbe placementTokenProbe) {
        PartitionKey sameTvfIdentity = new PartitionKey(sourceToken, tvfName);
        PartitionState sameTvfState = findPartitionState(taskSyncContext, sourceToken, tvfName);
        if (sameTvfState != null || finishedPartitions.contains(sameTvfIdentity)) {
            if (isMoveOutSatisfied(sameTvfState, sameTvfIdentity, moveInTimestamp, destToken, finishedPartitions)) {
                return true;
            }
        }

        if (tvfName != null && !InitialPartition.isInitialPartition(sourceToken)) {
            List<PartitionState> crossTvfStates = findPartitionStatesAnyTvf(taskSyncContext, sourceToken);
            PartitionKey crossTvfFinishedKey = findFinishedPartitionAnyTvf(finishedPartitions, sourceToken);
            if (sameTvfState != null || !crossTvfStates.isEmpty() || crossTvfFinishedKey != null) {
                for (PartitionState crossTvfState : crossTvfStates) {
                    if (isMoveOutSatisfied(crossTvfState, crossTvfState.getKey(), moveInTimestamp, destToken, finishedPartitions)) {
                        return true;
                    }
                }
                if (crossTvfFinishedKey != null
                        && isMoveOutSatisfied(null, crossTvfFinishedKey, moveInTimestamp, destToken, finishedPartitions)) {
                    return true;
                }
                return false;
            }

            if (placementTokenProbe != null
                    && placementTokenProbe.isExternalPlacementToken(sourceToken, moveInTimestamp)) {
                LOGGER.info("Source partition {} does not belong to any configured placement TVF, "
                        + "treating cross-placement MoveIn as satisfied for destination {} (tvf={})",
                        sourceToken, destToken, tvfName);
                return true;
            }
        }

        return false;
    }

    /**
     * Returns {@code true} if any partition in {@code taskSyncContext} has an unresolved
     * {@code MoveInState} listing {@code sourceToken} while {@code lastPublishedProcessedTs}
     * has not yet advanced past that MoveIn timestamp and {@code eventTimestamp} advances
     * {@code lastPublishedProcessedTs}.
     */
    public static boolean shouldAdvanceProcessedTimestampForMoveIn(TaskSyncContext taskSyncContext,
                                                                   String sourceToken,
                                                                   String sourceTvfName,
                                                                   Timestamp eventTimestamp,
                                                                   Timestamp lastPublishedProcessedTs) {
        if (taskSyncContext == null || eventTimestamp == null) {
            return false;
        }
        if (lastPublishedProcessedTs != null && eventTimestamp.compareTo(lastPublishedProcessedTs) <= 0) {
            return false;
        }
        Set<PartitionKey> finished = null;
        PartitionKey sourceIdentity = new PartitionKey(sourceToken, sourceTvfName);
        PartitionState sourceState = null;
        boolean sourceStateResolved = false;
        for (PartitionState candidate : getAllPartitions(taskSyncContext)) {
            if (candidate.getMoveInState() == null
                    || candidate.getMoveInState().getTimestamp() == null
                    || candidate.getMoveInState().getSourcePartitionTokens() == null) {
                continue;
            }
            Timestamp moveInTs = candidate.getMoveInState().getTimestamp();
            if (!candidate.getMoveInState().getSourcePartitionTokens().contains(sourceToken)) {
                continue;
            }
            if (lastPublishedProcessedTs != null && lastPublishedProcessedTs.compareTo(moveInTs) > 0) {
                continue;
            }
            if (finished == null) {
                finished = getFinishedPartitions(taskSyncContext);
            }
            if (!sourceStateResolved) {
                sourceState = findPartitionState(taskSyncContext, sourceToken, sourceTvfName);
                sourceStateResolved = true;
            }
            if (!isMoveOutSatisfied(sourceState, sourceIdentity, moveInTs, candidate.getToken(), finished)) {
                return true;
            }
        }
        return false;
    }

    private static List<PartitionState> getAllPartitions(TaskSyncContext taskSyncContext) {
        List<PartitionState> all = new ArrayList<>();
        all.addAll(taskSyncContext.getCurrentTaskState().getPartitions());
        all.addAll(taskSyncContext.getCurrentTaskState().getSharedPartitions());
        for (TaskState ts : taskSyncContext.getTaskStates().values()) {
            all.addAll(ts.getPartitions());
            all.addAll(ts.getSharedPartitions());
        }
        return all;
    }

    private static boolean isMoveOutSatisfied(PartitionState sourceState,
                                              PartitionKey sourceIdentity,
                                              Timestamp moveInTimestamp,
                                              String destToken,
                                              Set<PartitionKey> finishedPartitions) {
        List<MoveOutState> moveOutStates = sourceState == null ? List.of() : sourceState.getMoveOutStates();
        boolean satisfiedByMoveOutState = moveOutStates.stream()
                .anyMatch(mos -> {
                    int cmp = mos.getTimestamp().compareTo(moveInTimestamp);
                    return cmp > 0 || (cmp == 0 && mos.getDestPartitionTokens().contains(destToken));
                });
        if (satisfiedByMoveOutState) {
            return true;
        }
        if (finishedPartitions.contains(sourceIdentity)) {
            LOGGER.info("Source partition {} already finished/removed, treating MoveOut as satisfied for destination {}",
                    sourceIdentity, destToken);
            return true;
        }
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

    /** Searches all task states (partitions and shared partitions) for a matching token across any TVF. */
    public static PartitionState findPartitionStateAnyTvf(TaskSyncContext taskSyncContext, String token) {
        List<PartitionState> matches = findPartitionStatesAnyTvf(taskSyncContext, token);
        return matches.isEmpty() ? null : matches.get(0);
    }

    /** Returns all partition states across any TVF matching {@code token}. */
    public static List<PartitionState> findPartitionStatesAnyTvf(TaskSyncContext taskSyncContext, String token) {
        List<PartitionState> result = new ArrayList<>();
        for (PartitionState ps : taskSyncContext.getCurrentTaskState().getPartitions()) {
            if (ps.getToken().equals(token)) {
                result.add(ps);
            }
        }
        for (PartitionState ps : taskSyncContext.getCurrentTaskState().getSharedPartitions()) {
            if (ps.getToken().equals(token)) {
                result.add(ps);
            }
        }
        for (TaskState ts : taskSyncContext.getTaskStates().values()) {
            for (PartitionState ps : ts.getPartitions()) {
                if (ps.getToken().equals(token)) {
                    result.add(ps);
                }
            }
            for (PartitionState ps : ts.getSharedPartitions()) {
                if (ps.getToken().equals(token)) {
                    result.add(ps);
                }
            }
        }
        return result;
    }

    private static PartitionKey findFinishedPartitionAnyTvf(Set<PartitionKey> finishedPartitions, String token) {
        for (PartitionKey key : finishedPartitions) {
            if (key.getToken().equals(token)) {
                return key;
            }
        }
        return null;
    }

    private static boolean matches(PartitionState partition, String token, String tvfName) {
        return partition.getToken().equals(token) && Objects.equals(partition.getTvfName(), tvfName);
    }

}
