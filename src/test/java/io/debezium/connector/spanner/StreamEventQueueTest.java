/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashSet;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.model.event.FinishPartitionEvent;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

class StreamEventQueueTest {

    @Test
    void testStreamEventQueue() throws InterruptedException {
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        doNothing().when(metricsEventPublisher).publishMetricEvent(any());

        StreamEventQueue streamEventQueue = new StreamEventQueue(3, metricsEventPublisher);
        HashSet<String> parentTokens = new HashSet<>();
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(1L);

        FinishPartitionEvent partitionEvent = new FinishPartitionEvent(new Partition("token", parentTokens, startTimestamp, Timestamp.ofTimeMicroseconds(1L)));
        streamEventQueue.put(partitionEvent);
        ChangeStreamEvent takeEvent = streamEventQueue.take();
        assertSame(partitionEvent, takeEvent);

        verify(metricsEventPublisher, times(3)).publishMetricEvent(any());
    }
}
