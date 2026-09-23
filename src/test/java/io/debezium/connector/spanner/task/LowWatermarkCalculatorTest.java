/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;

class LowWatermarkCalculatorTest {

    @Test
    void returnsEarliestStartTimestampAcrossPlacementInitialPartitions() {
        Timestamp earliest = Timestamp.ofTimeMicroseconds(100L);
        PartitionState east = initialPartition("tvfEast", Timestamp.ofTimeMicroseconds(200L));
        PartitionState west = initialPartition("tvfWest", earliest);
        TaskState taskState = TaskState.builder()
                .taskUid("task0")
                .partitions(List.of(east, west))
                .sharedPartitions(List.of())
                .build();
        TaskSyncContext context = mock(TaskSyncContext.class);
        when(context.isInitialized()).thenReturn(true);
        when(context.getAllTaskStates()).thenReturn(Map.of("task0", taskState));
        when(context.getTaskUid()).thenReturn("task0");
        TaskSyncContextHolder contextHolder = mock(TaskSyncContextHolder.class);
        when(contextHolder.get()).thenReturn(context);
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.getHeartbeatInterval()).thenReturn(Duration.ofSeconds(10));
        PartitionOffsetProvider offsetProvider = mock(PartitionOffsetProvider.class);

        Timestamp lowWatermark = new LowWatermarkCalculator(config, contextHolder, offsetProvider)
                .calculateLowWatermark(false);

        assertEquals(earliest, lowWatermark);
        verifyNoInteractions(offsetProvider);
    }

    @Test
    void ignoresLegacyNullTvfInitialPartitionWhenPlacementRootsExist() {
        Timestamp earliestPlacementStart = Timestamp.ofTimeMicroseconds(100L);
        PartitionState legacy = initialPartition(null, Timestamp.ofTimeMicroseconds(1L));
        PartitionState east = initialPartition("tvfEast", Timestamp.ofTimeMicroseconds(200L));
        PartitionState west = initialPartition("tvfWest", earliestPlacementStart);
        PartitionOffsetProvider offsetProvider = mock(PartitionOffsetProvider.class);

        Timestamp lowWatermark = calculator(
                taskState(List.of(legacy, east, west), List.of()), offsetProvider, List.of("tvfEast", "tvfWest"))
                .calculateLowWatermark(false);

        assertEquals(earliestPlacementStart, lowWatermark);
        verifyNoInteractions(offsetProvider);
    }

    @Test
    void keepsNullTvfInitialPartitionWhenPlacementTvfsAreNotConfigured() {
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(100L);
        PartitionState initial = initialPartition(null, startTimestamp);
        PartitionOffsetProvider offsetProvider = mock(PartitionOffsetProvider.class);

        Timestamp lowWatermark = calculator(taskState(List.of(initial), List.of()), offsetProvider)
                .calculateLowWatermark(false);

        assertEquals(startTimestamp, lowWatermark);
        verifyNoInteractions(offsetProvider);
    }

    @Test
    void ignoresFinishedSharedPartition() {
        Timestamp activeOffset = Timestamp.ofTimeMicroseconds(500L);
        PartitionState active = partition("active", "tvfEast", Timestamp.ofTimeMicroseconds(100L), PartitionStateEnum.RUNNING);
        PartitionState finishedShared = partition("stale", "tvfWest", Timestamp.ofTimeMicroseconds(1L), PartitionStateEnum.FINISHED);
        PartitionOffsetProvider offsetProvider = mock(PartitionOffsetProvider.class);
        when(offsetProvider.getOffsets(anyCollection())).thenReturn(Map.of(active.getKey(), activeOffset));

        Timestamp lowWatermark = calculator(taskState(List.of(active), List.of(finishedShared)), offsetProvider)
                .calculateLowWatermark(false);

        assertEquals(activeOffset, lowWatermark);
    }

    @Test
    void ignoresRemovedSharedInitialPartition() {
        Timestamp activeOffset = Timestamp.ofTimeMicroseconds(500L);
        PartitionState active = partition("active", "tvfEast", Timestamp.ofTimeMicroseconds(100L), PartitionStateEnum.RUNNING);
        PartitionState removedParent = partition(InitialPartition.PARTITION_TOKEN, "tvfWest", Timestamp.ofTimeMicroseconds(1L), PartitionStateEnum.REMOVED);
        PartitionOffsetProvider offsetProvider = mock(PartitionOffsetProvider.class);
        when(offsetProvider.getOffsets(anyCollection())).thenReturn(Map.of(active.getKey(), activeOffset));

        Timestamp lowWatermark = calculator(taskState(List.of(active), List.of(removedParent)), offsetProvider)
                .calculateLowWatermark(false);

        assertEquals(activeOffset, lowWatermark);
    }

    @Test
    void calculatesOffsetsIndependentlyForSameTokenAcrossTvfs() {
        PartitionState east = partition("shared-token", "tvfEast", Timestamp.ofTimeMicroseconds(100L), PartitionStateEnum.RUNNING);
        PartitionState west = partition("shared-token", "tvfWest", Timestamp.ofTimeMicroseconds(100L), PartitionStateEnum.RUNNING);
        Timestamp eastOffset = Timestamp.ofTimeMicroseconds(500L);
        Timestamp westOffset = Timestamp.ofTimeMicroseconds(300L);
        PartitionOffsetProvider offsetProvider = mock(PartitionOffsetProvider.class);
        when(offsetProvider.getOffsets(anyCollection())).thenReturn(Map.of(
                east.getKey(), eastOffset,
                west.getKey(), westOffset));

        Timestamp lowWatermark = calculator(taskState(List.of(east, west), List.of()), offsetProvider)
                .calculateLowWatermark(false);

        assertEquals(westOffset, lowWatermark);
    }

    private static LowWatermarkCalculator calculator(TaskState taskState, PartitionOffsetProvider offsetProvider) {
        return calculator(taskState, offsetProvider, List.of());
    }

    private static LowWatermarkCalculator calculator(TaskState taskState, PartitionOffsetProvider offsetProvider, List<String> placementTvfNames) {
        TaskSyncContext context = mock(TaskSyncContext.class);
        when(context.isInitialized()).thenReturn(true);
        when(context.getAllTaskStates()).thenReturn(Map.of("task0", taskState));
        when(context.getTaskUid()).thenReturn("task0");
        TaskSyncContextHolder contextHolder = mock(TaskSyncContextHolder.class);
        when(contextHolder.get()).thenReturn(context);
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.getHeartbeatInterval()).thenReturn(Duration.ofSeconds(10));
        when(config.placementTvfNames()).thenReturn(placementTvfNames);
        return new LowWatermarkCalculator(config, contextHolder, offsetProvider);
    }

    private static TaskState taskState(List<PartitionState> partitions, List<PartitionState> sharedPartitions) {
        return TaskState.builder()
                .taskUid("task0")
                .partitions(partitions)
                .sharedPartitions(sharedPartitions)
                .build();
    }

    private static PartitionState initialPartition(String tvfName, Timestamp startTimestamp) {
        return partition(InitialPartition.PARTITION_TOKEN, tvfName, startTimestamp, PartitionStateEnum.CREATED);
    }

    private static PartitionState partition(String token, String tvfName, Timestamp startTimestamp, PartitionStateEnum state) {
        return PartitionState.builder()
                .token(token)
                .tvfName(tvfName)
                .startTimestamp(startTimestamp)
                .state(state)
                .parents(Set.of())
                .build();
    }
}
