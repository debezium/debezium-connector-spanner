/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.MoveOutState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Records the {@link MoveOutState} for a source partition that has processed a MoveOut event.
 * A source partition never pauses for its own MoveOut events - it keeps streaming and can
 * report several distinct MoveOuts (at different commit timestamps, to different destinations)
 * over its lifetime, each of which a paused destination may still be waiting on.
 *
 * <p>Incoming MoveOut state is merged with existing entries rather than being blindly appended:
 * each destination token is always kept at the latest (or equal) timestamp it has been associated
 * with, and the resulting entries are sorted by ascending timestamp. This prevents stale entries
 * for the same destination from piling up while preserving independent moves to different
 * destinations. Triggers a sync-topic publish so that destination partitions can observe the
 * updated {@link PartitionState#getMoveOutStates()} via the sync topic.
 */
public class MoveOutStateUpdateOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(MoveOutStateUpdateOperation.class);

    private final String token;
    private final Timestamp commitTimestamp;
    private final List<String> destinationTokens;

    public MoveOutStateUpdateOperation(String token, Timestamp commitTimestamp, List<String> destinationTokens) {
        this.token = token;
        this.commitTimestamp = commitTimestamp;
        this.destinationTokens = destinationTokens;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return true;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();

        MoveOutState newMoveOutState = new MoveOutState(commitTimestamp, destinationTokens);

        List<PartitionState> updatedPartitions = currentTaskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getToken().equals(token)) {
                        return partitionState.toBuilder()
                                .moveOutStates(mergeMoveOutStates(partitionState.getMoveOutStates(), newMoveOutState))
                                .build();
                    }
                    return partitionState;
                })
                .collect(Collectors.toList());

        LOGGER.info("Task {}, MoveOut state updated: partition={}, commitTimestamp={}, destinations={}",
                taskSyncContext.getTaskUid(), token, commitTimestamp, destinationTokens);

        return taskSyncContext.toBuilder()
                .currentTaskState(currentTaskState.toBuilder().partitions(updatedPartitions).build())
                .build();
    }

    /**
     * Merges the incoming {@link MoveOutState} with existing ones. For every destination token
     * the latest timestamp wins (incoming timestamp is accepted when it is equal or later than the
     * existing one). Existing entries are preserved when they still hold destinations; genuinely
     * new destinations are appended to the entry for their timestamp. The result is sorted by
     * ascending commit timestamp so that later MoveOuts appear after earlier ones.
     */
    static List<MoveOutState> mergeMoveOutStates(List<MoveOutState> existing, MoveOutState incoming) {
        if (existing.isEmpty()) {
            return List.of(incoming);
        }

        List<MoveOutState> allStates = new ArrayList<>(existing);
        allStates.add(incoming);

        Map<String, Timestamp> latestByDest = allStates.stream()
                .flatMap(state -> state.getDestPartitionTokens().stream()
                        .map(dest -> Map.entry(dest, state.getTimestamp())))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (current, update) -> update.compareTo(current) >= 0 ? update : current,
                        LinkedHashMap::new));

        return latestByDest.entrySet().stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toCollection(HashSet::new))))
                .entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> new MoveOutState(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }
}
