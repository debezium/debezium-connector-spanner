/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.HashSet;
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

    private final Map<String, HashSet<String>> emittedRecords = new ConcurrentHashMap<>();
    private final Map<String, Boolean> partitionPendingFinish = new ConcurrentHashMap<>();

    public FinishingPartitionManager(BlockingConsumer<String> finishedPartitionConsumer) {
        this.finishedPartitionConsumer = finishedPartitionConsumer;
    }

    public void newRecord(String token, String recordUid) {
        synchronized (emittedRecords) {
            LOGGER.debug("Emitted new record {} for token {}", recordUid, token);
            if (emittedRecords.containsKey(token)) {
                HashSet<String> recordUids = emittedRecords.get(token);
                recordUids.add(recordUid);
                emittedRecords.put(token, recordUids);
            }
            else {
                HashSet<String> recordUids = new HashSet<>();
                recordUids.add(recordUid);
                emittedRecords.put(token, recordUids);
            }
        }
    }

    public void registerPartition(String token) {
        partitionPendingFinish.put(token, false);
    }

    public void commitRecord(String token, String recordUid) throws InterruptedException {
        LOGGER.debug("Committed new record {} for token {}", recordUid, token);
        Boolean pendingFinishFlag = partitionPendingFinish.get(token);

        if (pendingFinishFlag == null) {
            LOGGER.warn("Partition has not been registered to finish or already finished {}", token);
            return;
        }

        synchronized (emittedRecords) {
            if (emittedRecords.get(token) == null) {
                LOGGER.warn("Token {} does not seem to have emitted any records but has commited record {}, forcefinishing", token, recordUid);
                return;
            }

            HashSet<String> recordUids = emittedRecords.get(token);
            boolean containsRecord = recordUids.remove(recordUid);
            if (!containsRecord) {
                LOGGER.warn("Record {} was committed but does not seem to have been emitted or was already committed for token {}", recordUid, token);
            }
            emittedRecords.put(token, recordUids);

            if (pendingFinishFlag) {
                if (emittedRecords.get(token).isEmpty()) {
                    LOGGER.info("Forcing the token to be finished {}", token);
                    forceFinish(token);
                    LOGGER.info("Finished forcing the token to be finished {}", token);
                }
                else {
                    LOGGER.info("Committed another record {} for token {}, but emitted records {} are not empty", recordUid, token, emittedRecords.get(token));
                }
            }
        }
    }

    public void onPartitionFinishEvent(String token) throws InterruptedException {
        LOGGER.info("onPartitionFinishEvent: {}", token);

        Boolean pendingFinishFlag = partitionPendingFinish.get(token);

        if (pendingFinishFlag == null) {
            LOGGER.warn("Partition has not been registered to finish or already finished {}", token);
            return;
        }

        synchronized (emittedRecords) {
            if (emittedRecords.get(token) == null || emittedRecords.get(token).isEmpty()) {
                LOGGER.info("Forcing the token to be finished {}", token);
                forceFinish(token);
                LOGGER.info("Finished forcing the token to be finished {}", token);
            }
            else {
                LOGGER.info("Trying to finish token {}, but emittedRecords {} are not empty", token, emittedRecords.get(token));
                partitionPendingFinish.put(token, true);
            }
        }
    }

    public void forceFinish(String token) throws InterruptedException {
        synchronized (emittedRecords) {
            finishedPartitionConsumer.accept(token);

            partitionPendingFinish.remove(token);
            emittedRecords.remove(token);
        }
    }

    public Set<String> getPendingFinishPartitions() {
        return partitionPendingFinish.entrySet().stream()
                .filter(entry -> Boolean.TRUE.equals(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

}
