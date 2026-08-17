/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.task.state.MoveOutNotificationEvent;

class TaskStateChangeEventHandlerTest {

    @Test
    void testProcessEventMoveOutNotificationDoesNotThrow() throws InterruptedException {
        TaskSyncContextHolder taskSyncContextHolder = mock(TaskSyncContextHolder.class);
        TaskSyncContext taskSyncContext = mock(TaskSyncContext.class);
        when(taskSyncContextHolder.get()).thenReturn(taskSyncContext);
        when(taskSyncContext.getTaskUid()).thenReturn("task-uid-1");

        TaskStateChangeEventHandler handler = new TaskStateChangeEventHandler(
                taskSyncContextHolder,
                mock(io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher.class),
                mock(io.debezium.connector.spanner.db.stream.ChangeStream.class),
                mock(PartitionFactory.class),
                mock(io.debezium.connector.spanner.processor.SpannerEventDispatcher.class),
                () -> {
                },
                mock(io.debezium.connector.spanner.SpannerConnectorConfig.class),
                ex -> {
                });

        Timestamp ts = Timestamp.ofTimeMicroseconds(123L);
        MoveOutNotificationEvent event = new MoveOutNotificationEvent("partitionA", ts, List.of("destB"));

        assertDoesNotThrow(() -> handler.processEvent(event));
    }

    @Test
    void testProcessEventUnknownTypeThrowsIllegalState() {
        TaskSyncContextHolder taskSyncContextHolder = mock(TaskSyncContextHolder.class);
        TaskSyncContext taskSyncContext = mock(TaskSyncContext.class);
        when(taskSyncContextHolder.get()).thenReturn(taskSyncContext);
        when(taskSyncContext.getTaskUid()).thenReturn("task-uid-1");

        TaskStateChangeEventHandler handler = new TaskStateChangeEventHandler(
                taskSyncContextHolder,
                mock(io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher.class),
                mock(io.debezium.connector.spanner.db.stream.ChangeStream.class),
                mock(PartitionFactory.class),
                mock(io.debezium.connector.spanner.processor.SpannerEventDispatcher.class),
                () -> {
                },
                mock(io.debezium.connector.spanner.SpannerConnectorConfig.class),
                ex -> {
                });

        io.debezium.connector.spanner.task.state.TaskStateChangeEvent unknownEvent = new io.debezium.connector.spanner.task.state.TaskStateChangeEvent() {
        };

        assertThrows(IllegalStateException.class, () -> handler.processEvent(unknownEvent));
    }
}
