/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.List;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.Partition;

/**
 * Provides API for operations on Spanner partitions
 */
public interface PartitionManager {

    void newChildPartitions(List<Partition> partitions) throws InterruptedException;

    void updateToFinished(String token) throws InterruptedException;

    default void updateToFinished(String token, String tvfName) throws InterruptedException {
        updateToFinished(token);
    }

    void updateToRunning(String token) throws InterruptedException;

    default void updateToRunning(String token, String tvfName) throws InterruptedException {
        updateToRunning(token);
    }

    void updateToReadyForStreaming(String token) throws InterruptedException;

    default void updateToReadyForStreaming(String token, String tvfName) throws InterruptedException {
        updateToReadyForStreaming(token);
    }

    void notifyMoveOut(String token, Timestamp commitTimestamp, List<String> destinationTokens) throws InterruptedException;

    default void notifyMoveOut(String token, String tvfName, Timestamp commitTimestamp, List<String> destinationTokens) throws InterruptedException {
        notifyMoveOut(token, commitTimestamp, destinationTokens);
    }

    void notifyMoveIn(String token, Timestamp commitTimestamp, String recordSequence, List<String> sourcePartitionTokens) throws InterruptedException;

    default void notifyMoveIn(String token, String tvfName, Timestamp commitTimestamp, String recordSequence, List<String> sourcePartitionTokens)
            throws InterruptedException {
        notifyMoveIn(token, commitTimestamp, recordSequence, sourcePartitionTokens);
    }

    /**
     * Buffer-gate path: publishes the MoveIn state to the sync topic without transitioning
     * the partition to {@code CREATED}.  The streaming thread remains alive and handles its
     * own gate.
     *
     * @param isFirstMoveIn {@code true} for the first MoveIn in a buffer sequence
     */
    void publishMoveInStateOnly(String token, Timestamp commitTimestamp, String recordSequence,
                                List<String> sourcePartitionTokens, boolean isFirstMoveIn)
            throws InterruptedException;

    default void publishMoveInStateOnly(String token, String tvfName, Timestamp commitTimestamp, String recordSequence,
                                        List<String> sourcePartitionTokens, boolean isFirstMoveIn)
            throws InterruptedException {
        publishMoveInStateOnly(token, commitTimestamp, recordSequence, sourcePartitionTokens, isFirstMoveIn);
    }

    void updateProcessedTimestamp(String token, Timestamp processedTimestamp, String lastBoundaryRecordSequence) throws InterruptedException;

    default void updateProcessedTimestamp(String token, String tvfName, Timestamp processedTimestamp,
                                          String lastBoundaryRecordSequence)
            throws InterruptedException {
        updateProcessedTimestamp(token, processedTimestamp, lastBoundaryRecordSequence);
    }

}
