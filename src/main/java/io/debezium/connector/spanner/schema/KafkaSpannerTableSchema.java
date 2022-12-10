/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.schema;

import java.util.function.Function;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.model.Mod;
import io.debezium.data.Envelope;
import io.debezium.schema.DataCollectionSchema;

/**
 * Kafka record schema for Spanner DB table
 */
public class KafkaSpannerTableSchema implements DataCollectionSchema {

    private final TableId id;
    private final Schema keySchema;
    private final Envelope envelopeSchema;
    private final Schema valueSchema;
    private final Function<Mod, Struct> keyGenerator;
    private final Function<Mod, Struct> oldValueStructGenerator;
    private final Function<Mod, Struct> newValueStructGenerator;

    public KafkaSpannerTableSchema(TableId id, Schema keySchema, Function<Mod, Struct> keyGenerator,
                                   Envelope envelopeSchema, Schema valueSchema,
                                   Function<Mod, Struct> oldValueStructGenerator,
                                   Function<Mod, Struct> newValueStructGenerator) {
        this.id = id;
        this.keySchema = keySchema;
        this.envelopeSchema = envelopeSchema;
        this.valueSchema = valueSchema;
        this.keyGenerator = keyGenerator;
        this.oldValueStructGenerator = oldValueStructGenerator;
        this.newValueStructGenerator = newValueStructGenerator;
    }

    @Override
    public TableId id() {
        return id;
    }

    public Schema valueSchema() {
        return valueSchema;
    }

    @Override
    public Schema keySchema() {
        return keySchema;
    }

    @Override
    public Envelope getEnvelopeSchema() {
        return envelopeSchema;
    }

    public Struct getKeyStructFromMod(Mod mod) {
        return mod == null ? null : keyGenerator.apply(mod);
    }

    public Struct getOldValueStructFromMod(Mod mod) {
        return mod == null ? null : oldValueStructGenerator.apply(mod);
    }

    public Struct getNewValueStructFromMod(Mod mod) {
        return mod == null ? null : newValueStructGenerator.apply(mod);
    }

}
