/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.db.model.StreamEventMetadata;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.stream.exception.ChangeStreamException;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.function.BlockingConsumer;

class PartitionQueryingMonitorTest {

    @Test
    void testStart() {
        PartitionThreadPool partitionThreadPool = spy(new PartitionThreadPool());
        doReturn(Set.of("1", "2")).when(partitionThreadPool).getActiveThreads();
        BlockingConsumer<String> onStuckPartitionConsumer = (BlockingConsumer<String>) mock(BlockingConsumer.class);
        Consumer<ChangeStreamException> errorConsumer = (Consumer<ChangeStreamException>) mock(Consumer.class);

        PartitionQueryingMonitor partitionQueryingMonitor = spy(new PartitionQueryingMonitor(
                partitionThreadPool, Duration.ofMillis(1), onStuckPartitionConsumer, errorConsumer,
                new MetricsEventPublisher(), 2));

        partitionQueryingMonitor.stop();
        partitionQueryingMonitor.start();
        int stuckHeartbeatIntervals = partitionQueryingMonitor.stuckHeartbeatIntervals(Instant.now().minusSeconds(5));
        Assertions.assertTrue(partitionQueryingMonitor.isPartitionStuck(Instant.now().minusSeconds(5)));
        Assertions.assertTrue(stuckHeartbeatIntervals > 0);
    }

    @Test
    void testAcceptStreamEvent() {
        PartitionThreadPool partitionThreadPool = new PartitionThreadPool();
        BlockingConsumer<String> onStuckPartitionConsumer = (BlockingConsumer<String>) mock(BlockingConsumer.class);
        Consumer<ChangeStreamException> errorConsumer = (Consumer<ChangeStreamException>) mock(Consumer.class);
        PartitionQueryingMonitor partitionQueryingMonitor = new PartitionQueryingMonitor(
                partitionThreadPool, Duration.ofSeconds(1), onStuckPartitionConsumer,
                errorConsumer, new MetricsEventPublisher(), 3);

        ChangeStreamEvent changeStreamEvent = mock(ChangeStreamEvent.class);
        StreamEventMetadata streamEventMetadata = mock(StreamEventMetadata.class);

        doReturn(streamEventMetadata).when(changeStreamEvent).getMetadata();
        doReturn("token").when(streamEventMetadata).getPartitionToken();

        partitionQueryingMonitor.acceptStreamEvent(changeStreamEvent);

        verify(changeStreamEvent).getMetadata();
        verify(streamEventMetadata).getPartitionToken();
    }

}
