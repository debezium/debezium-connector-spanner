/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor.heartbeat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.LoggingContext;

class SpannerHeartbeatTest {

    private final SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
    private final ChangeEventQueue<DataChangeEvent> changeEventQueue = new ChangeEventQueue.Builder<DataChangeEvent>()
            .pollInterval(Duration.ofMillis(1))
            .maxBatchSize(10)
            .maxQueueSize(10)
            .queueProvider(new DefaultQueueProvider<>(10))
            .loggingContextSupplier(() -> LoggingContext.forConnector("test", "test", "test"))
            .build();

    private final Map<String, String> partition = Map.of("partitionToken", "v1");

    private final SpannerHeartbeat underTest = new SpannerHeartbeat("Topic Name",
            schemaNameAdjuster,
            changeEventQueue, Clock.fixed(Instant.parse("2025-08-22T10:15:30.00Z"), ZoneOffset.UTC));

    @BeforeEach
    void setUp() {
        when(schemaNameAdjuster.adjust(any())).thenReturn("Adjust");
    }

    @Test
    void testHeartbeatWithNullOffset() throws InterruptedException {

        underTest.emitWithDelay(partition, generateOffset(null));

        assertThat(changeEventQueue.remainingCapacity()).isEqualTo(9);
        changeEventQueue.poll();
        verify(schemaNameAdjuster, atLeast(1)).adjust(any());
    }

    @Test
    void testHeartbeatWithEmptyOffset() throws InterruptedException {

        underTest.emitWithDelay(partition, generateOffset(Collections.emptyMap()));

        assertThat(changeEventQueue.remainingCapacity()).isEqualTo(9);
        changeEventQueue.poll();
        verify(schemaNameAdjuster, atLeast(1)).adjust(any());
    }

    @Test
    void testEmit() throws InterruptedException {

        underTest.emit(partition, generateOffset(Collections.emptyMap()));

        assertThat(changeEventQueue.remainingCapacity()).isEqualTo(9);
        changeEventQueue.poll();
        verify(schemaNameAdjuster, atLeast(1)).adjust(any());
    }

    @Test
    void testPartitionTokenKey() throws InterruptedException {

        underTest.emit(partition, generateOffset(Collections.emptyMap()));

        Struct key = (Struct) changeEventQueue.poll().get(0)
                .getRecord()
                .key();

        assertThat(key.get("partitionToken")).isEqualTo("v1");
        verify(schemaNameAdjuster, atLeast(1)).adjust(any());
    }

    @Test
    void testMessageValue() throws InterruptedException {

        underTest.emit(partition, generateOffset(Collections.emptyMap()));

        Struct value = (Struct) changeEventQueue.poll()
                .get(0)
                .getRecord()
                .value();

        assertThat(value.get("ts_ms")).isEqualTo(1755857730000L);
        verify(schemaNameAdjuster, atLeast(1)).adjust(any());
    }

    private OffsetContext generateOffset(final Map<String, ?> offset) {
        return new OffsetContext() {
            @Override
            public Map<String, ?> getOffset() {
                return offset;
            }

            @Override
            public Schema getSourceInfoSchema() {
                return null;
            }

            @Override
            public Struct getSourceInfo() {
                return null;
            }

            @Override
            public boolean isInitialSnapshotRunning() {
                return false;
            }

            @Override
            public void markSnapshotRecord(SnapshotRecord record) {

            }

            @Override
            public void preSnapshotStart(boolean onDemand) {

            }

            @Override
            public void preSnapshotCompletion() {

            }

            @Override
            public void postSnapshotCompletion() {

            }

            @Override
            public void event(DataCollectionId collectionId, Instant timestamp) {

            }

            @Override
            public TransactionContext getTransactionContext() {
                return null;
            }
        };
    }

}
