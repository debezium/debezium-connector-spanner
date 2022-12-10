/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.List;

import io.debezium.connector.spanner.db.model.Partition;

/**
 * Provides API for operations on Spanner partitions
 */
public interface PartitionManager {

    void newChildPartitions(List<Partition> partitions);

    void updateToFinished(String token);

    void updateToRunning(String token);

    void updateToReadyForStreaming(String token);

}
