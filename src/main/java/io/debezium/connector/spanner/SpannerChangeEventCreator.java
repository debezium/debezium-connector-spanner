/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Instant;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.spanner.processor.SourceRecordUtils;
import io.debezium.pipeline.DataChangeEvent;

/**
 * Creates Spanner Data Change Events
 */
public class SpannerChangeEventCreator implements io.debezium.pipeline.spi.ChangeEventCreator {
    @Override
    public DataChangeEvent createDataChangeEvent(SourceRecord sourceRecord) {
        return new DataChangeEvent(getRecord(sourceRecord));
    }

    private SourceRecord getRecord(SourceRecord record) {
        return SourceRecordUtils.addEmitTimestamp(record, Instant.now().toEpochMilli());
    }
}
