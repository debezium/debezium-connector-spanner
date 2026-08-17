/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.task.state.MoveOutNotificationEvent;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;
import io.debezium.function.BlockingConsumer;

class SynchronizedPartitionManagerTest {

    @Test
    @SuppressWarnings("unchecked")
    void testNotifyMoveOutPublishesMoveOutNotificationEvent() throws InterruptedException {
        BlockingConsumer<TaskStateChangeEvent> publisher = mock(BlockingConsumer.class);
        SynchronizedPartitionManager manager = new SynchronizedPartitionManager(publisher);

        Timestamp ts = Timestamp.ofTimeMicroseconds(500L);
        List<String> destinations = List.of("d1", "d2");

        manager.notifyMoveOut("tokenX", ts, destinations);

        verify(publisher).accept(argThat(event -> {
            if (!(event instanceof MoveOutNotificationEvent)) {
                return false;
            }
            MoveOutNotificationEvent moveOut = (MoveOutNotificationEvent) event;
            return "tokenX".equals(moveOut.getToken())
                    && ts.equals(moveOut.getCommitTimestamp())
                    && destinations.equals(moveOut.getDestinationTokens());
        }));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNotifyMoveOutTokenAndDestinationsArePropagated() throws InterruptedException {
        BlockingConsumer<TaskStateChangeEvent> publisher = mock(BlockingConsumer.class);
        SynchronizedPartitionManager manager = new SynchronizedPartitionManager(publisher);

        Timestamp ts = Timestamp.ofTimeSecondsAndNanos(1_000L, 0);
        List<String> destinations = List.of("alpha", "beta", "gamma");

        manager.notifyMoveOut("sourcePartition", ts, destinations);

        ArgumentCaptor<TaskStateChangeEvent> captor = ArgumentCaptor.forClass(TaskStateChangeEvent.class);
        verify(publisher).accept(captor.capture());

        TaskStateChangeEvent captured = captor.getValue();
        org.junit.jupiter.api.Assertions.assertInstanceOf(MoveOutNotificationEvent.class, captured);
        MoveOutNotificationEvent event = (MoveOutNotificationEvent) captured;
        org.junit.jupiter.api.Assertions.assertEquals("sourcePartition", event.getToken());
        org.junit.jupiter.api.Assertions.assertEquals(ts, event.getCommitTimestamp());
        org.junit.jupiter.api.Assertions.assertEquals(destinations, event.getDestinationTokens());
    }
}
