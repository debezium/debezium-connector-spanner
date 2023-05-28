/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.node.MissingNode;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.model.Mod;
import io.debezium.connector.spanner.db.model.ModType;
import io.debezium.connector.spanner.schema.KafkaSpannerTableSchema;
import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.util.Clock;

class SpannerChangeRecordEmitterTest {

    @Test
    void testConstructor() {
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        Mod mod = new Mod(0, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance());

        SpannerPartition initialSpannerPartition = SpannerPartition.getInitialSpannerPartition();
        Configuration config = mock(Configuration.class);
        when(config.getString(anyString())).thenReturn("String");
        when(config.getString((Field) any())).thenReturn("String");
        when(config.asProperties()).thenReturn(new Properties());
        SpannerChangeRecordEmitter actualSpannerChangeRecordEmitter = new SpannerChangeRecordEmitter(
                "1234", ModType.INSERT, mod, initialSpannerPartition, mock(SpannerOffsetContext.class), mock(Clock.class),
                new SpannerConnectorConfig(config));

        SpannerPartition partition = actualSpannerChangeRecordEmitter.getPartition();
        assertSame(initialSpannerPartition, partition);
        assertEquals(Envelope.Operation.CREATE, actualSpannerChangeRecordEmitter.getOperation());
        Map<String, String> sourcePartition = partition.getSourcePartition();
        assertEquals(1, sourcePartition.size());
        assertEquals("Parent0", sourcePartition.get("partitionToken"));
        assertTrue(partition.getLoggingContext().isEmpty());
        assertEquals("Parent0", partition.getValue());
    }

    private static Stream<Arguments> summaryStringProvider() {
        return Stream.of(
                Arguments.of(ModType.INSERT, Envelope.Operation.CREATE),
                Arguments.of(ModType.UPDATE, Envelope.Operation.UPDATE),
                Arguments.of(ModType.DELETE, Envelope.Operation.DELETE));
    }

    @ParameterizedTest
    @MethodSource("summaryStringProvider")
    void testGetOperation(ModType modType, Envelope.Operation expected) {
        Mod mod = new Mod(0, MissingNode.getInstance(), MissingNode.getInstance(), MissingNode.getInstance());
        Configuration config = mock(Configuration.class);
        when(config.getString(anyString())).thenReturn("String");
        when(config.getString((Field) any())).thenReturn("String");
        when(config.asProperties()).thenReturn(new Properties());
        assertEquals(expected,
                new SpannerChangeRecordEmitter(
                        "1234", modType, mod, SpannerPartition.getInitialSpannerPartition(),
                        mock(SpannerOffsetContext.class), mock(Clock.class), new SpannerConnectorConfig(config)).getOperation());
    }

    @Test
    void testEmitChangeRecords() throws InterruptedException {
        SpannerOffsetContext spannerOffsetContext = mock(SpannerOffsetContext.class);
        when(spannerOffsetContext.getSourceInfo()).thenReturn(null);
        Clock clock = mock(Clock.class);
        LocalDateTime atStartOfDayResult = LocalDate.of(1970, 1, 1).atStartOfDay();
        when(clock.currentTimeAsInstant()).thenReturn(atStartOfDayResult.atZone(ZoneId.of("UTC")).toInstant());
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        Mod mod = new Mod(0, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance());

        Configuration config = mock(Configuration.class);
        when(config.getString(anyString())).thenReturn("String");
        when(config.getString((Field) any())).thenReturn("String");
        when(config.asProperties()).thenReturn(new Properties());

        SpannerChangeRecordEmitter spannerChangeRecordEmitter = spy(new SpannerChangeRecordEmitter("1234", ModType.INSERT,
                mod, SpannerPartition.getInitialSpannerPartition(), spannerOffsetContext, clock, new SpannerConnectorConfig(config)));
        Function<Mod, Struct> function = (Function<Mod, Struct>) mock(Function.class);
        when(function.apply(any())).thenThrow(new IllegalArgumentException());
        TableId id = TableId.getTableId("Table Name");
        ConnectSchema keySchema = new ConnectSchema(Schema.Type.INT8);
        Envelope envelopeSchema = new Envelope(new ConnectSchema(Schema.Type.INT8));

        KafkaSpannerTableSchema kafkaSpannerTableSchema = new KafkaSpannerTableSchema(
                id, keySchema, function, envelopeSchema, new ConnectSchema(Schema.Type.INT8),
                (Function<Mod, Struct>) mock(Function.class), (Function<Mod, Struct>) mock(Function.class));

        doNothing().when(spannerChangeRecordEmitter).emitCreateRecord(any(), any());
        doNothing().when(spannerChangeRecordEmitter).emitUpdateRecord(any(), any());
        doNothing().when(spannerChangeRecordEmitter).emitDeleteRecord(any(), any());

        when(spannerChangeRecordEmitter.getOperation()).thenReturn(Envelope.Operation.CREATE);
        spannerChangeRecordEmitter.emitChangeRecords(kafkaSpannerTableSchema, null);
        verify(spannerChangeRecordEmitter).emitCreateRecord(any(), any());

        when(spannerChangeRecordEmitter.getOperation()).thenReturn(Envelope.Operation.UPDATE);
        spannerChangeRecordEmitter.emitChangeRecords(kafkaSpannerTableSchema, null);
        verify(spannerChangeRecordEmitter).emitUpdateRecord(any(), any());

        when(spannerChangeRecordEmitter.getOperation()).thenReturn(Envelope.Operation.DELETE);
        spannerChangeRecordEmitter.emitChangeRecords(kafkaSpannerTableSchema, null);
        verify(spannerChangeRecordEmitter).emitDeleteRecord(any(), any());

        when(spannerChangeRecordEmitter.getOperation()).thenReturn(Envelope.Operation.READ);
        assertThrows(IllegalArgumentException.class,
                () -> spannerChangeRecordEmitter.emitChangeRecords(kafkaSpannerTableSchema, null));
    }

    @Test
    void testEmitCreateRecord() throws InterruptedException {
        SpannerOffsetContext spannerOffsetContext = mock(SpannerOffsetContext.class);
        when(spannerOffsetContext.getSourceInfo()).thenReturn(null);
        Clock clock = mock(Clock.class);
        LocalDateTime atStartOfDayResult = LocalDate.of(1970, 1, 1).atStartOfDay();
        when(clock.currentTimeAsInstant()).thenReturn(atStartOfDayResult.atZone(ZoneId.of("UTC")).toInstant());
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        Mod mod = new Mod(0, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance());

        Configuration config = mock(Configuration.class);
        when(config.getString(anyString())).thenReturn("String");
        when(config.getString((Field) any())).thenReturn("String");
        when(config.asProperties()).thenReturn(new Properties());

        SpannerChangeRecordEmitter spannerChangeRecordEmitter = spy(new SpannerChangeRecordEmitter("1234", ModType.INSERT,
                mod, SpannerPartition.getInitialSpannerPartition(), spannerOffsetContext, clock, new SpannerConnectorConfig(config)));
        Function<Mod, Struct> function = (Function<Mod, Struct>) mock(Function.class);
        when(function.apply(any())).thenThrow(new IllegalArgumentException());
        TableId id = TableId.getTableId("Table Name");
        ConnectSchema keySchema = new ConnectSchema(Schema.Type.STRUCT);
        Envelope envelopeSchema = new Envelope(new ConnectSchema(Schema.Type.STRUCT));

        KafkaSpannerTableSchema kafkaSpannerTableSchema = spy(new KafkaSpannerTableSchema(
                id, keySchema, function, envelopeSchema, new ConnectSchema(Schema.Type.STRUCT),
                (Function<Mod, Struct>) mock(Function.class), (Function<Mod, Struct>) mock(Function.class)));

        ChangeRecordEmitter.Receiver<SpannerPartition> receiver = (ChangeRecordEmitter.Receiver<SpannerPartition>) mock(ChangeRecordEmitter.Receiver.class);
        doNothing().when(receiver).changeRecord(any(), any(), any(), any(), any(), any(), any());

        doReturn(null).when(kafkaSpannerTableSchema).getKeyStructFromMod(any());
        doReturn(null).when(kafkaSpannerTableSchema).getNewValueStructFromMod(any());
        doReturn(null).when(spannerChangeRecordEmitter).getEnvelopeCreate(any(), any());
        doReturn(null).when(spannerChangeRecordEmitter).getEnvelopeUpdate(any(), any(), any());
        doReturn(null).when(spannerChangeRecordEmitter).getEnvelopeDelete(any(), any());

        spannerChangeRecordEmitter.emitCreateRecord(receiver, kafkaSpannerTableSchema);
        spannerChangeRecordEmitter.emitUpdateRecord(receiver, kafkaSpannerTableSchema);
        spannerChangeRecordEmitter.emitDeleteRecord(receiver, kafkaSpannerTableSchema);
        assertThrows(UnsupportedOperationException.class,
                () -> spannerChangeRecordEmitter.emitReadRecord(receiver, kafkaSpannerTableSchema));

        verify(receiver, times(3)).changeRecord(any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void testGetEnvelopeThrows() {
        SpannerOffsetContext spannerOffsetContext = mock(SpannerOffsetContext.class);
        when(spannerOffsetContext.getSourceInfo()).thenReturn(null);
        Clock clock = mock(Clock.class);
        when(clock.currentTimeAsInstant()).thenThrow(new IllegalArgumentException());
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        Mod mod = new Mod(0, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance());

        Configuration config = mock(Configuration.class);
        when(config.getString(anyString())).thenReturn("String");
        when(config.getString((Field) any())).thenReturn("String");
        when(config.asProperties()).thenReturn(new Properties());

        SpannerChangeRecordEmitter spannerChangeRecordEmitter = new SpannerChangeRecordEmitter("1234", ModType.INSERT,
                mod, SpannerPartition.getInitialSpannerPartition(), spannerOffsetContext, clock, new SpannerConnectorConfig(config));
        TableId id = TableId.getTableId("Table Name");
        ConnectSchema keySchema = new ConnectSchema(Schema.Type.INT8);
        Function<Mod, Struct> keyGenerator = (Function<Mod, Struct>) mock(Function.class);
        Envelope envelopeSchema = new Envelope(new ConnectSchema(Schema.Type.INT8));
        KafkaSpannerTableSchema kafkaSpannerTableSchema = new KafkaSpannerTableSchema(id, keySchema, keyGenerator,
                envelopeSchema, new ConnectSchema(Schema.Type.INT8), (Function<Mod, Struct>) mock(Function.class),
                (Function<Mod, Struct>) mock(Function.class));

        assertThrows(IllegalArgumentException.class,
                () -> spannerChangeRecordEmitter.getEnvelopeCreate(kafkaSpannerTableSchema, null));
        assertThrows(IllegalArgumentException.class,
                () -> spannerChangeRecordEmitter.getEnvelopeUpdate(kafkaSpannerTableSchema, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> spannerChangeRecordEmitter.getEnvelopeDelete(kafkaSpannerTableSchema, null));

        verify(spannerOffsetContext, times(3)).getSourceInfo();
        verify(clock, times(3)).currentTimeAsInstant();
    }

    @Test
    void testEmitReadRecord() {
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        Mod mod = new Mod(0, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance());

        Configuration config = mock(Configuration.class);
        when(config.getString(anyString())).thenReturn("String");
        when(config.getString((Field) any())).thenReturn("String");
        when(config.asProperties()).thenReturn(new Properties());

        SpannerChangeRecordEmitter spannerChangeRecordEmitter = new SpannerChangeRecordEmitter("1234", ModType.INSERT,
                mod, SpannerPartition.getInitialSpannerPartition(), mock(SpannerOffsetContext.class), mock(Clock.class), new SpannerConnectorConfig(config));
        TableId id = TableId.getTableId("Table Name");
        ConnectSchema keySchema = new ConnectSchema(Schema.Type.INT8);
        Function<Mod, Struct> keyGenerator = (Function<Mod, Struct>) mock(Function.class);
        Envelope envelopeSchema = new Envelope(new ConnectSchema(Schema.Type.INT8));
        assertThrows(UnsupportedOperationException.class,
                () -> spannerChangeRecordEmitter.emitReadRecord(null,
                        new KafkaSpannerTableSchema(id, keySchema, keyGenerator, envelopeSchema,
                                new ConnectSchema(Schema.Type.INT8), (Function<Mod, Struct>) mock(Function.class),
                                (Function<Mod, Struct>) mock(Function.class))));
    }
}
