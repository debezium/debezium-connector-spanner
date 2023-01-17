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

import io.debezium.function.BlockingConsumer;

/**
 * Tracking Finish State of a Partition when handling kafka connect commit, finish event.
 * Sending a notification to the {@code finishedPartitionConsumer}
 */
public class FinishingPartitionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(FinishingPartitionManager.class);

    private final BlockingConsumer<String> finishedPartitionConsumer;

    private final Map<String, String> lastEmittedRecord = new ConcurrentHashMap<>();
    private final Map<String, Boolean> partitionPendingFinish = new ConcurrentHashMap<>();

    private final Map<String, String> lastCommittedRecord = new ConcurrentHashMap<>();

    public FinishingPartitionManager(BlockingConsumer<String> finishedPartitionConsumer) {
        this.finishedPartitionConsumer = finishedPartitionConsumer;
    }

    public String newRecord(String token) {
        String recordUid = lastEmittedRecord.get(token) == null ? "aaaaaaaa" : next(lastEmittedRecord.get(token));
        lastEmittedRecord.put(token, recordUid);
        return recordUid;
    }

    public void registerPartition(String token) {
        partitionPendingFinish.put(token, false);
    }

    public void commitRecord(String token, String recordUid) throws InterruptedException {
        Boolean pendingFinishFlag = partitionPendingFinish.get(token);

        if (pendingFinishFlag == null) {
            LOGGER.warn("Partition has not been registered to finish or already finished {}", token);
            return;
        }

        if (!pendingFinishFlag) {
            if (lastCommittedRecord.get(token) == null) {
                lastCommittedRecord.put(token, recordUid);
            }
            else {
                if (recordUid.compareTo(lastCommittedRecord.get(token)) > 0) {
                    lastCommittedRecord.put(token, recordUid);
                }
            }
            return;
        }

        if (lastEmittedRecord.get(token) == null || lastEmittedRecord.get(token).equals(recordUid)) {
            LOGGER.info("Finished forcing the token to be finished {}", token);
            forceFinish(token);
        }
    }

    public void onPartitionFinishEvent(String token) throws InterruptedException {
        LOGGER.info("onPartitionFinishEvent: {}", token);

        Boolean pendingFinishFlag = partitionPendingFinish.get(token);

        if (pendingFinishFlag == null) {
            LOGGER.warn("Partition has not been registered to finish or already finished {}", token);
            return;
        }

        if (lastEmittedRecord.get(token) == null || lastEmittedRecord.get(token).equals(lastCommittedRecord.get(token))) {
            LOGGER.info("Forcing the token to be finished {}", token);
            forceFinish(token);
            LOGGER.info("Finished forcing the token to be finished {}", token);
        }
        else {
            LOGGER.info(
                    "Cannot finish the token {} due to lastCommittedRecord {} not being equal to"
                            + " lastEmittedRecord {}",
                    token,
                    lastCommittedRecord.get(token),
                    lastEmittedRecord.get(token));
            partitionPendingFinish.put(token, true);
        }
    }

    public void forceFinish(String token) throws InterruptedException {
        finishedPartitionConsumer.accept(token);

        partitionPendingFinish.remove(token);
        lastEmittedRecord.remove(token);
        lastCommittedRecord.remove(token);
    }

    public Set<String> getPendingFinishPartitions() {
        return partitionPendingFinish.entrySet().stream()
                .filter(entry -> entry.getValue().equals(true))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    public Set<String> getPendingPartitions() {
        return partitionPendingFinish.entrySet().stream()
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
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
