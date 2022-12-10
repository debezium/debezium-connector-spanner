/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Field;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.connector.spanner.processor.SourceRecordUtils;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.util.Collect;

/**
 * Provides basic functionality of the Spanner SourceTask implementations
 */
public abstract class SpannerBaseSourceTask
        extends BaseSourceTask<SpannerPartition, SpannerOffsetContext> {

    protected SpannerChangeEventSourceCoordinator coordinator;

    private final List<SourceRecord> records = new LinkedList<>();

    @Override
    public void commitRecord(SourceRecord sourceRecord, RecordMetadata metadata) throws InterruptedException {
        super.commitRecord(sourceRecord, metadata);

        synchronized (this) {
            records.add(sourceRecord);
        }

        if (metadata != null) {
            sourceRecord = SourceRecordUtils.addPublishTimestamp(sourceRecord, Instant.now().toEpochMilli());
        }

        this.onRecordSent(sourceRecord);
    }

    protected abstract void onRecordSent(SourceRecord sourceRecord);

    @Override
    public void commit() throws InterruptedException {
        super.commit();

        if (coordinator == null) {
            return;
        }
        synchronized (this) {
            coordinator.commitRecords(records);
            records.clear();
        }
    }

    protected Offsets<SpannerPartition, SpannerOffsetContext> getInitialOffsets() {
        return Offsets.of(Collect.hashMapOf(SpannerPartition.getInitialSpannerPartition(), null));
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return SpannerConnectorConfig.ALL_FIELDS;
    }

    @Override
    public String version() {
        return Module.version();
    }
}
