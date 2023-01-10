/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.schema;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.MissingNode;

import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.model.Mod;
import io.debezium.data.Envelope;

class KafkaSpannerTableSchemaTest {

    @Test
    void testGetKeyStructFromModIsNull() {
        TableId id = TableId.getTableId("Table Name");
        ConnectSchema keySchema = new ConnectSchema(Schema.Type.INT8);
        Function<Mod, Struct> keyGenerator = (Function<Mod, Struct>) mock(Function.class);
        Envelope envelopeSchema = new Envelope(new ConnectSchema(Schema.Type.INT8));
        assertNull(
                new KafkaSpannerTableSchema(id, keySchema, keyGenerator, envelopeSchema, new ConnectSchema(Schema.Type.INT8),
                        (Function<Mod, Struct>) mock(Function.class), (Function<Mod, Struct>) mock(Function.class))
                        .getKeyStructFromMod(null));
    }

    @Test
    void testGetKeyStructFromMod() {
        Function<Mod, Struct> function = (Function<Mod, Struct>) mock(Function.class);
        when(function.apply(any())).thenReturn(null);
        TableId id = TableId.getTableId("Table Name");
        ConnectSchema keySchema = new ConnectSchema(Schema.Type.INT8);
        Envelope envelopeSchema = new Envelope(new ConnectSchema(Schema.Type.INT8));
        KafkaSpannerTableSchema kafkaSpannerTableSchema = new KafkaSpannerTableSchema(id, keySchema, function,
                envelopeSchema, new ConnectSchema(Schema.Type.INT8), (Function<Mod, Struct>) mock(Function.class),
                (Function<Mod, Struct>) mock(Function.class));
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        assertNull(kafkaSpannerTableSchema
                .getKeyStructFromMod(new Mod(0, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance())));
        verify(function).apply(any());
    }

    @Test
    void testGetOldValueStructFromModIsNull() {
        TableId id = TableId.getTableId("Table Name");
        ConnectSchema keySchema = new ConnectSchema(Schema.Type.INT8);
        Function<Mod, Struct> keyGenerator = (Function<Mod, Struct>) mock(Function.class);
        Envelope envelopeSchema = new Envelope(new ConnectSchema(Schema.Type.INT8));
        assertNull(
                (new KafkaSpannerTableSchema(id, keySchema, keyGenerator, envelopeSchema, new ConnectSchema(Schema.Type.INT8),
                        (Function<Mod, Struct>) mock(Function.class), (Function<Mod, Struct>) mock(Function.class)))
                        .getOldValueStructFromMod(null));
    }

    @Test
    void testGetOldValueStructFromMod() {
        Function<Mod, Struct> function = (Function<Mod, Struct>) mock(Function.class);
        when(function.apply(any())).thenReturn(null);
        TableId id = TableId.getTableId("Table Name");
        ConnectSchema keySchema = new ConnectSchema(Schema.Type.INT8);
        Function<Mod, Struct> keyGenerator = (Function<Mod, Struct>) mock(Function.class);
        Envelope envelopeSchema = new Envelope(new ConnectSchema(Schema.Type.INT8));
        KafkaSpannerTableSchema kafkaSpannerTableSchema = new KafkaSpannerTableSchema(id, keySchema, keyGenerator,
                envelopeSchema, new ConnectSchema(Schema.Type.INT8), function, (Function<Mod, Struct>) mock(Function.class));
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        assertNull(kafkaSpannerTableSchema
                .getOldValueStructFromMod(new Mod(0, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance())));
        verify(function).apply(any());
    }

    @Test
    void testGetNewValueStructFromModIsNull() {
        TableId id = TableId.getTableId("Table Name");
        ConnectSchema keySchema = new ConnectSchema(Schema.Type.INT8);
        Function<Mod, Struct> keyGenerator = (Function<Mod, Struct>) mock(Function.class);
        Envelope envelopeSchema = new Envelope(new ConnectSchema(Schema.Type.INT8));
        assertNull(
                (new KafkaSpannerTableSchema(id, keySchema, keyGenerator, envelopeSchema, new ConnectSchema(Schema.Type.INT8),
                        (Function<Mod, Struct>) mock(Function.class), (Function<Mod, Struct>) mock(Function.class)))
                        .getNewValueStructFromMod(null));
    }

    @Test
    void testGetNewValueStructFromMod() {
        Function<Mod, Struct> function = (Function<Mod, Struct>) mock(Function.class);
        when(function.apply(any())).thenReturn(null);
        TableId id = TableId.getTableId("Table Name");
        ConnectSchema keySchema = new ConnectSchema(Schema.Type.INT8);
        Function<Mod, Struct> keyGenerator = (Function<Mod, Struct>) mock(Function.class);
        Envelope envelopeSchema = new Envelope(new ConnectSchema(Schema.Type.INT8));
        KafkaSpannerTableSchema kafkaSpannerTableSchema = new KafkaSpannerTableSchema(id, keySchema, keyGenerator,
                envelopeSchema, new ConnectSchema(Schema.Type.INT8), (Function<Mod, Struct>) mock(Function.class), function);
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        assertNull(kafkaSpannerTableSchema
                .getNewValueStructFromMod(new Mod(0, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance())));
        verify(function).apply(any());
    }
}
