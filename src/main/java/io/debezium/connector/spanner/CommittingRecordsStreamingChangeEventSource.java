/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

public interface CommittingRecordsStreamingChangeEventSource<P extends Partition, O extends OffsetContext>
        extends io.debezium.pipeline.source.spi.StreamingChangeEventSource<P, O> {

    default void commitRecords(List<SourceRecord> recordList) {
    }
}
