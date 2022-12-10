/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import io.debezium.connector.spanner.db.model.Partition;

/**
 * A listener for the various state querying partition. Used in {@link ChangeStream}
 */
public interface PartitionEventListener {
    void onRun(Partition partition);

    void onFinish(Partition partition);

    void onException(Partition partition, Exception ex);

    boolean onStuckPartition(String token);
}
