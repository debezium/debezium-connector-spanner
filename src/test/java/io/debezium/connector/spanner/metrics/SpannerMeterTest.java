/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import com.google.cloud.Timestamp;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerConnectorTask;
import io.debezium.connector.spanner.SpannerErrorHandler;
import io.debezium.connector.spanner.function.BlockingSupplier;
import io.debezium.connector.spanner.metrics.latency.LatencyCalculator;
import io.debezium.connector.spanner.metrics.latency.Statistics;
import io.debezium.relational.TableId;

class SpannerMeterTest {

    @Test
    void testConstructor() throws InterruptedException {
        SpannerConnectorTask task = new SpannerConnectorTask();
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(configuration);
        BlockingSupplier<Timestamp> lowWatermarkSupplier = (BlockingSupplier<Timestamp>) mock(BlockingSupplier.class);
        Configuration configuration1 = mock(Configuration.class);
        when(configuration1.getString((Field) any())).thenReturn("String");
        when(configuration1.asProperties()).thenReturn(new Properties());
        SpannerErrorHandler spannerErrorHandler = new SpannerErrorHandler(mock(SpannerConnectorTask.class),
                (ChangeEventQueue<?>) mock(ChangeEventQueue.class));
        SpannerMeter actualSpannerMeter = new SpannerMeter(task, connectorConfig, spannerErrorHandler, lowWatermarkSupplier);

        assertTrue(actualSpannerMeter.getCapturedTables().isEmpty());
        assertNull(actualSpannerMeter.getTaskUid());
        assertNull(actualSpannerMeter.getLowWatermarkLag());
        assertEquals(0, actualSpannerMeter.getNumberOfActiveQueries());
        assertEquals(0, actualSpannerMeter.getErrorCount());
        assertEquals(0, actualSpannerMeter.getNumberOfPartitionsDetected());
        assertEquals(0, actualSpannerMeter.getNumberOfQueriesIssuedTotal());
        Statistics connectorLatency = actualSpannerMeter.getConnectorLatency();
        assertNull(connectorLatency.getMinValue());
        Statistics commitToEmitLatency = actualSpannerMeter.getCommitToEmitLatency();
        assertEquals(null, commitToEmitLatency.getValueAtP95());
        assertNull(commitToEmitLatency.getMinValue());
        Statistics totalLatency = actualSpannerMeter.getTotalLatency();
        assertEquals(null, totalLatency.getValueAtP50());
        assertNull(totalLatency.getMinValue());
        Statistics lowWatermarkLagLatency = actualSpannerMeter.getLowWatermarkLagLatency();
        assertNull(lowWatermarkLagLatency.getMaxValue());
        assertEquals(null, lowWatermarkLagLatency.getValueAtP95());
        assertNull(actualSpannerMeter.getEmitToPublishLatency().getAverageValue());
        assertEquals(null, connectorLatency.getValueAtP50());
    }

    @Test
    void testCaptureTable() {
        SpannerConnectorConfig connectorConfig = Mockito.mock(SpannerConnectorConfig.class);

        SpannerMeter spannerMeter = new SpannerMeter(new SpannerConnectorTask(), connectorConfig, null, () -> null);
        spannerMeter.captureTable(new TableId("Catalog Name", "Schema Name", "Table Name"));
        assertEquals(1, spannerMeter.getCapturedTables().size());
    }

    @Test
    void testReset() {
        SpannerConnectorConfig connectorConfig = Mockito.mock(SpannerConnectorConfig.class);

        SpannerMeter spannerMeter = new SpannerMeter(new SpannerConnectorTask(), connectorConfig, null, () -> null);
        spannerMeter.captureTable(new TableId("Catalog Name", "Schema Name", "Table Name"));
        assertEquals(1, spannerMeter.getCapturedTables().size());
        spannerMeter.reset();
        assertTrue(spannerMeter.getCapturedTables().isEmpty());
    }

    @Test
    void testGetTaskUid() {
        SpannerConnectorConfig connectorConfig = Mockito.mock(SpannerConnectorConfig.class);

        assertNull(new SpannerMeter(new SpannerConnectorTask(), connectorConfig, null, () -> null).getTaskUid());
    }

    private static Stream<Arguments> lowWatermarkLagProvider() {
        return Stream.of(
                Arguments.of(true, LatencyCalculator.getTimeBehindLowWatermark(Timestamp.ofTimeMicroseconds(0L))),
                Arguments.of(false, null));
    }

    @ParameterizedTest
    @MethodSource("lowWatermarkLagProvider")
    void testGetLowWatermarkLag(Boolean lowWatermarkEnabled, Long expected) throws InterruptedException {
        SpannerConnectorConfig connectorConfig = mock(SpannerConnectorConfig.class);
        when(connectorConfig.isLowWatermarkEnabled()).thenReturn(lowWatermarkEnabled);
        SpannerMeter spannerMeter = new SpannerMeter(
                new SpannerConnectorTask(), connectorConfig, null, () -> Timestamp.ofTimeMicroseconds(0L));
        Long lowWatermarkLag = spannerMeter.getLowWatermarkLag();
        if (expected == null) {
            assertNull(lowWatermarkLag);
        }
        else {
            assertTrue(lowWatermarkLag >= expected);
        }
    }

    private static Stream<Arguments> lowWatermarkProvider() {
        return Stream.of(
                Arguments.of(true, Timestamp.ofTimeMicroseconds(0L)),
                Arguments.of(false, null));
    }

    @ParameterizedTest
    @MethodSource("lowWatermarkProvider")
    void testGetLowWatermark(Boolean lowWatermarkEnabled, Timestamp expected) throws InterruptedException {
        SpannerConnectorConfig connectorConfig = mock(SpannerConnectorConfig.class);
        when(connectorConfig.isLowWatermarkEnabled()).thenReturn(lowWatermarkEnabled);
        SpannerMeter spannerMeter = new SpannerMeter(new SpannerConnectorTask(), connectorConfig, null,
                () -> Timestamp.ofTimeMicroseconds(0L));
        Timestamp lowWatermark = spannerMeter.getLowWatermark();
        assertEquals(expected, lowWatermark);
    }

    @Test
    void testGetNumberOfPartitionsDetected() {
        SpannerConnectorConfig connectorConfig = mock(SpannerConnectorConfig.class);
        SpannerMeter spannerMeter = new SpannerMeter(new SpannerConnectorTask(), connectorConfig, null, () -> null);
        assertEquals(0, spannerMeter.getNumberOfPartitionsDetected());
    }

    @Test
    void testGetNumberOfQueriesIssuedTotal() {
        SpannerConnectorConfig connectorConfig = mock(SpannerConnectorConfig.class);
        SpannerMeter spannerMeter = new SpannerMeter(new SpannerConnectorTask(), connectorConfig, null, () -> null);
        assertEquals(0, spannerMeter.getNumberOfQueriesIssuedTotal());
    }

    @Test
    void testGetNumberOfActiveQueries() {
        SpannerConnectorConfig connectorConfig = mock(SpannerConnectorConfig.class);
        SpannerMeter spannerMeter = new SpannerMeter(new SpannerConnectorTask(), connectorConfig, null, () -> null);
        assertEquals(0, spannerMeter.getNumberOfActiveQueries());
    }

    @Test
    void testGetErrorCount() {
        SpannerConnectorConfig connectorConfig = mock(SpannerConnectorConfig.class);
        SpannerMeter spannerMeter = new SpannerMeter(new SpannerConnectorTask(), connectorConfig, null, () -> null);
        assertEquals(0, spannerMeter.getErrorCount());
    }
}
