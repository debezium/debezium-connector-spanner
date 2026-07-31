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

    void updateToRunning(String token) throws InterruptedException;

    void updateToReadyForStreaming(String token) throws InterruptedException;

    void notifyMoveOut(String token, Timestamp commitTimestamp, List<String> destinationTokens) throws InterruptedException;

    void notifyMoveIn(String token, Timestamp commitTimestamp, String recordSequence, List<String> sourcePartitionTokens) throws InterruptedException;

    void updateProcessedTimestamp(String token, Timestamp processedTimestamp, String lastBoundaryRecordSequence) throws InterruptedException;

}
