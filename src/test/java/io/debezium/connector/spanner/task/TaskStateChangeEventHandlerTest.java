/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.kafka.internal.model.MoveInState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
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

    @Test
    void testProcessEventMoveOutNotificationSchedulesWaitingDestination() throws InterruptedException {
        Timestamp moveTimestamp = Timestamp.ofTimeSecondsAndNanos(1000, 0);

        PartitionState destPartition = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.CREATED)
                .parents(Set.of("src"))
                .moveInState(new MoveInState(moveTimestamp, "00001", List.of("src")))
                .build();
        PartitionState sourcePartition = PartitionState.builder()
                .token("src")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .build();

        TaskSyncContext initialContext = TaskSyncContext.builder()
                .taskUid("task-uid-1")
                .currentTaskState(TaskState.builder()
                        .taskUid("task-uid-1")
                        .partitions(List.of(destPartition, sourcePartition))
                        .sharedPartitions(List.of())
                        .build())
                .build();

        TaskSyncContextHolder taskSyncContextHolder = new TaskSyncContextHolder(mock(MetricsEventPublisher.class));
        taskSyncContextHolder.init(initialContext);

        Partition destStreamingPartition = Partition.builder()
                .token("dst")
                .startTimestamp(moveTimestamp)
                .parentTokens(Set.of("src"))
                .build();
        PartitionFactory partitionFactory = mock(PartitionFactory.class);
        when(partitionFactory.getPartitions(any())).thenReturn(Map.of("dst", destStreamingPartition));

        ChangeStream changeStream = mock(ChangeStream.class);
        when(changeStream.submitPartition(any())).thenReturn(true);

        TaskStateChangeEventHandler handler = new TaskStateChangeEventHandler(
                taskSyncContextHolder,
                mock(TaskSyncPublisher.class),
                changeStream,
                partitionFactory,
                mock(io.debezium.connector.spanner.processor.SpannerEventDispatcher.class),
                () -> {
                },
                mock(io.debezium.connector.spanner.SpannerConnectorConfig.class),
                ex -> {
                });

        MoveOutNotificationEvent event = new MoveOutNotificationEvent("src", moveTimestamp, List.of("dst"));

        handler.processEvent(event);
        // Drain the dedicated partition-scheduling executor before asserting: the offset fetch
        // and stream submission now run asynchronously off the event-processor thread.
        handler.shutdown();

        PartitionState updatedDest = taskSyncContextHolder.get().getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getToken().equals("dst"))
                .findFirst()
                .orElseThrow();
        PartitionState updatedSource = taskSyncContextHolder.get().getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getToken().equals("src"))
                .findFirst()
                .orElseThrow();

        assertEquals(1, updatedSource.getMoveOutStates().size());
        assertEquals(moveTimestamp, updatedSource.getMoveOutStates().get(0).getTimestamp());
        assertEquals(Set.of("dst"), updatedSource.getMoveOutStates().get(0).getDestPartitionTokens());
        assertEquals(PartitionStateEnum.SCHEDULED, updatedDest.getState(),
                "destination partition must be found and scheduled for streaming after the async offset-fetch completes");
    }

    @Test
    void testAsyncPartitionSchedulingReportsRuntimeException() throws InterruptedException {
        Timestamp moveTimestamp = Timestamp.ofTimeSecondsAndNanos(1000, 0);
        PartitionState destPartition = PartitionState.builder()
                .token("dst")
                .state(PartitionStateEnum.CREATED)
                .parents(Set.of("src"))
                .moveInState(new MoveInState(moveTimestamp, "00001", List.of("src")))
                .build();
        PartitionState sourcePartition = PartitionState.builder()
                .token("src")
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .build();
        TaskSyncContext initialContext = TaskSyncContext.builder()
                .taskUid("task-uid-1")
                .currentTaskState(TaskState.builder()
                        .taskUid("task-uid-1")
                        .partitions(List.of(destPartition, sourcePartition))
                        .sharedPartitions(List.of())
                        .build())
                .build();
        TaskSyncContextHolder taskSyncContextHolder = new TaskSyncContextHolder(mock(MetricsEventPublisher.class));
        taskSyncContextHolder.init(initialContext);

        RuntimeException schedulingFailure = new RuntimeException("offset lookup failed");
        PartitionFactory partitionFactory = mock(PartitionFactory.class);
        when(partitionFactory.getPartitions(any())).thenThrow(schedulingFailure);
        AtomicReference<RuntimeException> reportedFailure = new AtomicReference<>();
        TaskStateChangeEventHandler handler = new TaskStateChangeEventHandler(
                taskSyncContextHolder,
                mock(TaskSyncPublisher.class),
                mock(ChangeStream.class),
                partitionFactory,
                mock(io.debezium.connector.spanner.processor.SpannerEventDispatcher.class),
                () -> {
                },
                mock(io.debezium.connector.spanner.SpannerConnectorConfig.class),
                reportedFailure::set);

        handler.processEvent(new MoveOutNotificationEvent("src", moveTimestamp, List.of("dst")));
        handler.shutdown();

        assertSame(schedulingFailure, reportedFailure.get());
    }
}
