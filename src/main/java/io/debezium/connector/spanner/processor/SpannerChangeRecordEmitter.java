/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;

import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.connector.spanner.db.model.Mod;
import io.debezium.connector.spanner.db.model.ModType;
import io.debezium.connector.spanner.schema.KafkaSpannerTableSchema;
import io.debezium.data.Envelope;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;

/**
 * Represents a change applied to Spanner database and emits one or more corresponding change records.
 */
public class SpannerChangeRecordEmitter extends AbstractChangeRecordEmitter<SpannerPartition, KafkaSpannerTableSchema> {
    private final ModType modType;
    private final Mod mod;

    private final String recordUid;

    public SpannerChangeRecordEmitter(String recordUid, ModType modType, Mod mod, SpannerPartition partition,
                                      SpannerOffsetContext offsetContext, Clock clock) {
        super(partition, offsetContext, clock);
        this.modType = modType;
        this.mod = mod;
        this.recordUid = recordUid;
    }

    @Override
    public Envelope.Operation getOperation() {
        switch (modType) {
            case DELETE:
                return Envelope.Operation.DELETE;
            case INSERT:
                return Envelope.Operation.CREATE;
            case UPDATE:
                return Envelope.Operation.UPDATE;
        }
        throw new IllegalArgumentException("Unsupported operation: " + modType);
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver<SpannerPartition> receiver)
            throws InterruptedException {
        KafkaSpannerTableSchema tableSchema = (KafkaSpannerTableSchema) schema;
        Envelope.Operation operation = getOperation();

        switch (operation) {
            case CREATE:
                emitCreateRecord(receiver, tableSchema);
                break;
            case UPDATE:
                emitUpdateRecord(receiver, tableSchema);
                break;
            case DELETE:
                emitDeleteRecord(receiver, tableSchema);
                break;
            default:
                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
    }

    @Override
    protected void emitCreateRecord(Receiver<SpannerPartition> receiver, KafkaSpannerTableSchema tableSchema)
            throws InterruptedException {
        Struct newKey = tableSchema.getKeyStructFromMod(mod);
        Struct newValue = tableSchema.getNewValueStructFromMod(mod);
        Struct envelope = getEnvelopeCreate(tableSchema, newValue);

        receiver.changeRecord(getPartition(), tableSchema, Envelope.Operation.CREATE, newKey, envelope, getOffset(),
                getHeaders());
    }

    @VisibleForTesting
    Struct getEnvelopeCreate(KafkaSpannerTableSchema tableSchema, Struct newValue) {
        return tableSchema.getEnvelopeSchema().create(newValue, getOffset().getSourceInfo(),
                getClock().currentTimeAsInstant());
    }

    @Override
    protected void emitUpdateRecord(Receiver<SpannerPartition> receiver, KafkaSpannerTableSchema tableSchema)
            throws InterruptedException {

        Struct key = tableSchema.getKeyStructFromMod(mod);

        Struct newValue = tableSchema.getNewValueStructFromMod(mod);
        Struct oldValue = tableSchema.getOldValueStructFromMod(mod);

        Struct envelope = getEnvelopeUpdate(tableSchema, newValue, oldValue);
        receiver.changeRecord(getPartition(), tableSchema, Envelope.Operation.UPDATE, key, envelope, getOffset(), getHeaders());
    }

    @VisibleForTesting
    Struct getEnvelopeUpdate(KafkaSpannerTableSchema tableSchema, Struct newValue, Struct oldValue) {
        return tableSchema.getEnvelopeSchema().update(oldValue, newValue, getOffset().getSourceInfo(),
                getClock().currentTimeAsInstant());
    }

    @Override
    protected void emitDeleteRecord(Receiver<SpannerPartition> receiver, KafkaSpannerTableSchema tableSchema)
            throws InterruptedException {
        Struct oldKey = tableSchema.getKeyStructFromMod(mod);
        Struct oldValue = tableSchema.getOldValueStructFromMod(mod);

        Struct envelope = getEnvelopeDelete(tableSchema, oldValue);
        receiver.changeRecord(getPartition(), tableSchema, Envelope.Operation.DELETE, oldKey, envelope, getOffset(), getHeaders());
    }

    @VisibleForTesting
    Struct getEnvelopeDelete(KafkaSpannerTableSchema tableSchema, Struct oldValue) {
        return tableSchema.getEnvelopeSchema().delete(oldValue, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
    }

    @Override
    protected void emitReadRecord(Receiver<SpannerPartition> receiver, KafkaSpannerTableSchema tableSchema) {
        throw new UnsupportedOperationException("Unsupported read operation");
    }

    private ConnectHeaders getHeaders() {
        return SourceRecordUtils.from(recordUid);
    }

}
