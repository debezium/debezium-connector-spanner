/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.util.List;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.Partition;

/**
 * A listener for the various state querying partition. Used in {@link ChangeStream}
 */
public interface PartitionEventListener {
    void onRun(Partition partition) throws InterruptedException;

    void onFinish(Partition partition);

    void onException(Partition partition, Exception ex) throws InterruptedException;

    boolean onStuckPartition(String token) throws InterruptedException;

    void onWindowAdvanced(Partition partition, Timestamp windowEnd, String lastBoundaryRecordSequence) throws InterruptedException;

    void onMoveIn(Partition partition, Timestamp commitTimestamp, String recordSequence, List<String> sourcePartitionTokens) throws InterruptedException;
}
