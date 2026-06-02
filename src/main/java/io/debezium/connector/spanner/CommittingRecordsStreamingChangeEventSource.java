/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.List;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

public interface CommittingRecordsStreamingChangeEventSource<P extends Partition, O extends OffsetContext>
        extends io.debezium.pipeline.source.spi.StreamingChangeEventSource<P, O> {

    default void commitRecords(List<CommittedRecord> recordList) throws InterruptedException {
    }
}
