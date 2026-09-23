/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.db.model.PartitionKey;
import io.debezium.connector.spanner.task.TaskUid;
import io.debezium.function.BlockingConsumer;

/**
 * Tracking Finish State of a Partition when handling kafka connect commit, finish event.
 * Sending a notification to the {@code finishedPartitionConsumer}
 */
public class FinishingPartitionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(FinishingPartitionManager.class);

    private final FinishedPartitionConsumer finishedPartitionConsumer;
    private final SpannerConnectorConfig connectorConfig;

    private final Map<SpannerPartition, String> lastEmittedRecord = new ConcurrentHashMap<>();
    private final Map<SpannerPartition, Boolean> partitionPendingFinish = new ConcurrentHashMap<>();

    private final Map<SpannerPartition, String> lastCommittedRecord = new ConcurrentHashMap<>();
    private volatile String taskUid;

    public FinishingPartitionManager(SpannerConnectorConfig connectorConfig, BlockingConsumer<String> finishedPartitionConsumer) {
        this(connectorConfig, (token, tvfName) -> finishedPartitionConsumer.accept(token));
    }

    public FinishingPartitionManager(SpannerConnectorConfig connectorConfig, FinishedPartitionConsumer finishedPartitionConsumer) {
        this.finishedPartitionConsumer = finishedPartitionConsumer;
        this.connectorConfig = connectorConfig;
        this.taskUid = "";
        if (connectorConfig != null) {
            this.taskUid = TaskUid.generateTaskUid(connectorConfig.getConnectorName(), connectorConfig.getTaskId());
        }
    }

    public String newRecord(String token) {
        return newRecord(token, null);
    }

    public String newRecord(String token, String tvfName) {
        SpannerPartition partition = new SpannerPartition(token, tvfName);
        String recordUid = lastEmittedRecord.get(partition) == null ? "aaaaaaaa" : next(lastEmittedRecord.get(partition));
        lastEmittedRecord.put(partition, recordUid);
        return recordUid;
    }

    public void registerPartition(String token) {
        registerPartition(token, null);
    }

    public void registerPartition(String token, String tvfName) {
        partitionPendingFinish.put(new SpannerPartition(token, tvfName), false);
    }

    public void commitRecord(String token, String recordUid) throws InterruptedException {
        commitRecord(token, null, recordUid);
    }

    public void commitRecord(String token, String tvfName, String recordUid) throws InterruptedException {
        SpannerPartition partition = new SpannerPartition(token, tvfName);
        Boolean pendingFinishFlag = partitionPendingFinish.get(partition);

        if (pendingFinishFlag == null) {
            LOGGER.warn("Task: {}, Partition has not been registered to finish or already finished {} for task {}", taskUid, token);
            return;
        }

        if (!pendingFinishFlag) {
            if (lastCommittedRecord.get(partition) == null) {
                lastCommittedRecord.put(partition, recordUid);
            }
            else {
                if (recordUid.compareTo(lastCommittedRecord.get(partition)) > 0) {
                    lastCommittedRecord.put(partition, recordUid);
                }
            }
            return;
        }

        if (lastEmittedRecord.get(partition) == null || lastEmittedRecord.get(partition).equals(recordUid)) {
            LOGGER.info("Task: {}, Finished forcing the partition to be finished {}", taskUid, partition);
            forceFinish(token, tvfName);
        }
    }

    public void onPartitionFinishEvent(String token) throws InterruptedException {
        onPartitionFinishEvent(token, null);
    }

    public void onPartitionFinishEvent(String token, String tvfName) throws InterruptedException {
        SpannerPartition partition = new SpannerPartition(token, tvfName);
        LOGGER.info("Task: {}, onPartitionFinishEvent: {}", taskUid, partition);

        Boolean pendingFinishFlag = partitionPendingFinish.get(partition);

        if (pendingFinishFlag == null) {
            LOGGER.warn("Task: {}, Partition has not been registered to finish or already finished {}", taskUid, partition);
            return;
        }

        if (lastEmittedRecord.get(partition) == null || lastEmittedRecord.get(partition).equals(lastCommittedRecord.get(partition))) {
            LOGGER.info("Task: {}, Forcing the partition to be finished {}", taskUid, partition);
            forceFinish(token, tvfName);
            LOGGER.info("Task: {}, Done forcing the partition to be finished {}", taskUid, partition);
        }
        else {
            LOGGER.info(
                    "Task: {}, Cannot finish the partition {} due to lastCommittedRecord {} not being equal to"
                            + " lastEmittedRecord {}",
                    taskUid,
                    partition,
                    lastCommittedRecord.get(partition),
                    lastEmittedRecord.get(partition));
            partitionPendingFinish.put(partition, true);
        }
    }

    public void forceFinish(String token) throws InterruptedException {
        forceFinish(token, null);
    }

    public void forceFinish(String token, String tvfName) throws InterruptedException {
        SpannerPartition partition = new SpannerPartition(token, tvfName);
        finishedPartitionConsumer.accept(token, tvfName);

        partitionPendingFinish.remove(partition);
        lastEmittedRecord.remove(partition);
        lastCommittedRecord.remove(partition);
    }

    public Set<PartitionKey> getPendingFinishPartitions() {
        return partitionPendingFinish.entrySet().stream()
                .filter(entry -> entry.getValue().equals(true))
                .map(entry -> entry.getKey().getKey())
                .collect(Collectors.toSet());
    }

    public Set<PartitionKey> getPendingPartitions() {
        return partitionPendingFinish.keySet().stream()
                .map(SpannerPartition::getKey)
                .collect(Collectors.toSet());
    }

    @FunctionalInterface
    public interface FinishedPartitionConsumer {
        void accept(String token, String tvfName) throws InterruptedException;
    }

    private String next(String str) {
        // If string is empty.
        if (str.isEmpty()) {
            return "a";
        }

        // Find first character from right
        // which is not z.

        int i = str.length() - 1;
        while (i >= 0 && str.charAt(i) == 'z') {
            i--;
        }
        if (i == -1) {
            str = str + 'a';
        }
        else {
            String suffix = "";
            for (int j = i + 1; j < str.length(); j++) {
                suffix += 'a';
            }
            str = str.substring(0, i) + (char) ((int) (str.charAt(i)) + 1) + suffix;
        }
        return str;
    }

}
