/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerErrorHandler;
import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;

/** Calculates watermark based on offsets of all partitions */
public class LowWatermarkCalculator {
    private static final Logger LOGGER = LoggerFactory.getLogger(LowWatermarkCalculator.class);
    private static final long OFFSET_MONITORING_LAG_MAX_MS = 60_000;

    private final TaskSyncContextHolder taskSyncContextHolder;
    private final SpannerConnectorConfig spannerConnectorConfig;

    private final PartitionOffsetProvider partitionOffsetProvider;

    public LowWatermarkCalculator(
                                  SpannerConnectorConfig spannerConnectorConfig,
                                  TaskSyncContextHolder taskSyncContextHolder,
                                  PartitionOffsetProvider partitionOffsetProvider) {
        this.taskSyncContextHolder = taskSyncContextHolder;
        this.spannerConnectorConfig = spannerConnectorConfig;
        this.partitionOffsetProvider = partitionOffsetProvider;
    }

    public Timestamp calculateLowWatermark(boolean printOffsets) {

        TaskSyncContext taskSyncContext = taskSyncContextHolder.get();

        if (!taskSyncContext.isInitialized()) {
            LOGGER.warn("Task {}, TaskSyncContextHolder not initialized", taskSyncContextHolder.get().getTaskUid());
            return null;
        }

        Map<String, List<PartitionState>> partitionsMap = taskSyncContext.getAllTaskStates().values().stream()
                .flatMap(taskState -> taskState.getPartitions().stream())
                .filter(
                        partitionState -> !partitionState.getState().equals(PartitionStateEnum.FINISHED)
                                && !partitionState.getState().equals(PartitionStateEnum.REMOVED))
                .collect(Collectors.groupingBy(PartitionState::getToken));

        Set<String> duplicatesInPartitions = checkDuplication(partitionsMap);

        if (!duplicatesInPartitions.isEmpty()) {
            LOGGER.warn(
                    "Task {}, calculateLowWatermark: found duplication in partitionsMap: {}", taskSyncContextHolder.get().getTaskUid(), duplicatesInPartitions);
            return null;
        }

        Map<String, PartitionState> partitions = partitionsMap.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().get(0)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, List<PartitionState>> sharedPartitionsMap = taskSyncContext.getAllTaskStates().values().stream()
                .flatMap(taskState -> taskState.getSharedPartitions().stream())
                .filter(partitionState -> !partitions.containsKey(partitionState.getToken()))
                .collect(Collectors.groupingBy(PartitionState::getToken));

        Set<String> duplicatesInSharedPartitions = checkDuplication(sharedPartitionsMap);
        if (!duplicatesInSharedPartitions.isEmpty()) {
            LOGGER.warn(
                    "Task {}, calculateLowWatermark: found duplication in sharedPartitionsMap: {}",
                    taskSyncContextHolder.get().getTaskUid(), duplicatesInSharedPartitions);
            return null;
        }

        Map<String, PartitionState> sharedPartitions = sharedPartitionsMap.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().get(0)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, PartitionState> allPartitions = new HashMap<>();

        allPartitions.putAll(partitions);

        allPartitions.putAll(sharedPartitions);

        if (allPartitions.containsKey(InitialPartition.PARTITION_TOKEN)) {
            final long now = new Date().getTime();
            long lag = now
                    - allPartitions
                            .get(InitialPartition.PARTITION_TOKEN)
                            .getStartTimestamp()
                            .toDate()
                            .getTime();
            long acceptedLag = spannerConnectorConfig.getHeartbeatInterval().toMillis() + OFFSET_MONITORING_LAG_MAX_MS;
            if (lag > acceptedLag) {
                LOGGER.warn(
                        "task: {}, Partition has a very old start timestamp, lag: {}, token: {}",
                        taskSyncContextHolder.get().getTaskUid(),
                        lag,
                        InitialPartition.PARTITION_TOKEN);
            }
            return allPartitions.get(InitialPartition.PARTITION_TOKEN).getStartTimestamp();
        }

        Map<String, Timestamp> offsets;

        try {
            offsets = partitionOffsetProvider.getOffsets(allPartitions.keySet());
        }
        catch (ConnectException e) {
            if (e.getCause() != null && e.getCause() instanceof InterruptedException) {
                LOGGER.info(
                        "Task {}, Kafka connect offsetStorageReader is interrupting... Thread interrupted: {}",
                        taskSyncContextHolder.get().getTaskUid(), Thread.currentThread().isInterrupted());
            }
            else {
                LOGGER.warn(
                        "Task {}, Kafka connect offsetStorageReader cannot return offsets. Thread interrupted: {}. {}, throwing error",
                        taskSyncContextHolder.get().getTaskUid(), Thread.currentThread().isInterrupted(),
                        SpannerErrorHandler.getStackTrace(e));
                throw e;
            }
            return null;
        }
        catch (RuntimeException e) {
            LOGGER.warn(
                    "Task {}, Kafka connect offsetStorageReader cannot return offsets {}",
                    taskSyncContextHolder.get().getTaskUid(), SpannerErrorHandler.getStackTrace(e));
            return null;
        }

        if (printOffsets) {
            monitorOffsets(offsets, allPartitions);
        }

        if (printOffsets) {
            LOGGER.info("Task {}, Counted total number of partitions {}, owned partitions {}, shared partitions {}", taskSyncContext.getTaskUid(), allPartitions.size(),
                    partitions.size(),
                    sharedPartitions.size());
        }

        return allPartitions.values().stream()
                .map(
                        partitionState -> {
                            Timestamp timestamp = offsets.get(partitionState.getToken());
                            if (timestamp != null) {
                                return timestamp;
                            }
                            if (partitionState.getStartTimestamp() != null) {
                                return partitionState.getStartTimestamp();
                            }
                            throw new IllegalStateException(
                                    "lastCommitTimestamp or startTimestamp are not specified or offsets are empty");
                        })
                .min(Timestamp::compareTo)
                .orElse(spannerConnectorConfig.startTime());
    }

    private void monitorOffsets(Map<String, Timestamp> offsets, Map<String, PartitionState> allPartitions) {
        if (offsets == null) {
            return;
        }
        final long now = new Date().getTime();

        allPartitions.values().forEach(
                partitionState -> {
                    Timestamp timestamp = offsets.get(partitionState.getToken());
                    long acceptedLag = spannerConnectorConfig.getHeartbeatInterval().toMillis() + OFFSET_MONITORING_LAG_MAX_MS;
                    if (timestamp != null) {
                        String token = partitionState.getToken();
                        long lag = now - timestamp.toDate().getTime();
                        if (lag > acceptedLag) {
                            LOGGER.warn("Task {}, Partition has a very old offset, lag: {}, token: {}", taskSyncContextHolder.get().getTaskUid(), lag, partitionState);
                        }
                    }
                    else if (partitionState.getStartTimestamp() != null) {
                        String token = partitionState.getToken();
                        long lag = now - partitionState.getStartTimestamp().toDate().getTime();
                        if (lag > acceptedLag) {
                            LOGGER.warn("Task {}, Partition has a very old start time, lag: {}, token: {}", taskSyncContextHolder.get().getTaskUid(), lag,
                                    partitionState);
                        }
                    }
                });
    }

    private Set<String> checkDuplication(Map<String, List<PartitionState>> map) {
        return map.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }
}
