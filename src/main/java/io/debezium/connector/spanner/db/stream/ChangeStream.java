/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.util.function.BooleanSupplier;

import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.stream.exception.ChangeStreamException;

/**
 *  The {@code ChangeStream} interface should be implemented by class
 *  that querying partitions
 */
public interface ChangeStream {
    boolean submitPartition(Partition partition);

    void stop(String token);

    void stop();

    void run(BooleanSupplier runningFlagSupplier, ChangeStreamEventConsumer changeStreamEventConsumer,
             PartitionEventListener partitionEventListener)
            throws ChangeStreamException, InterruptedException;
}
