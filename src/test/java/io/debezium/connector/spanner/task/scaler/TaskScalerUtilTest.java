/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.scaler;

import static io.debezium.connector.spanner.task.TaskTestHelper.createTaskStateMap;
import static io.debezium.connector.spanner.task.TaskTestHelper.createTaskSyncEvent;
import static io.debezium.connector.spanner.task.TaskTestHelper.generatePartitions;
import static io.debezium.connector.spanner.task.TaskTestHelper.generateTaskStateWithPartitions;
import static io.debezium.connector.spanner.task.TaskTestHelper.generateTaskStateWithRandomPartitions;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

class TaskScalerUtilTest {

    @Test
    void partitionsInWorkCountTest() {
        List<PartitionState> partitions = Stream.of(PartitionStateEnum.values())
                .map(s -> PartitionState.builder()
                        .token(UUID.randomUUID().toString())
                        .state(s)
                        .build())
                .collect(toList());

        TaskState task1 = generateTaskStateWithPartitions(partitions.subList(0, 3));
        TaskState task2 = generateTaskStateWithPartitions(partitions.subList(3, partitions.size()));

        long partitionsInWorkCount = TaskScalerUtil.partitionsInWorkCount(createTaskSyncEvent(task1, task2));
        assertThat(partitionsInWorkCount)
                // there only 2 not in progress states
                .isEqualTo(partitions.size() - 2);
    }

    @Test
    void partitionsInWorkCount_nullSyncEventTest() {
        long partitionsInWorkCount = TaskScalerUtil.partitionsInWorkCount(null);
        assertThat(partitionsInWorkCount).isZero();
    }

    @Test
    void partitionsInWorkCount_noPartitionsTest() {
        Map<String, TaskState> taskStates = createTaskStateMap(generateTaskStateWithRandomPartitions(0, 0));
        TaskSyncEvent taskSyncEvent = TaskSyncEvent.builder().taskStates(taskStates).build();

        long partitionsInWorkCount = TaskScalerUtil.partitionsInWorkCount(taskSyncEvent);
        assertThat(partitionsInWorkCount).isZero();
    }

    @Test
    void tasksCountTest() {
        long tasksCount = TaskScalerUtil.tasksCount(createTaskSyncEvent(
                generateTaskStateWithRandomPartitions(0, 0),
                generateTaskStateWithRandomPartitions(2, 1),
                generateTaskStateWithRandomPartitions(1, 2)));
        assertThat(tasksCount).isEqualTo(3);
    }

    @Test
    void tasksCount_nullSyncEventTest() {
        long tasksCount = TaskScalerUtil.tasksCount(null);
        assertThat(tasksCount).isZero();
    }

    @Test
    void idlingTaskCount_tasksNoPartitions() {
        TaskState taskNoPartitions1 = generateTaskStateWithRandomPartitions(0, 0);
        TaskState taskNoPartitions2 = generateTaskStateWithRandomPartitions(0, 0);

        long idlingTaskCount = TaskScalerUtil.idlingTaskCount(createTaskSyncEvent(taskNoPartitions1, taskNoPartitions2));
        assertThat(idlingTaskCount).isEqualTo(2);
    }

    @Test
    void idlingTaskCount_tasksOnlyFinishedPartitions() {
        TaskState task1 = generateTaskStateWithPartitions(
                generatePartitions(2, () -> PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.FINISHED)
                        .build()));
        TaskState task2 = generateTaskStateWithPartitions(
                generatePartitions(3, () -> PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.FINISHED)
                        .build()));

        long idlingTaskCount = TaskScalerUtil.idlingTaskCount(createTaskSyncEvent(task1, task2));
        assertThat(idlingTaskCount).isEqualTo(2);
    }

    @Test
    void idlingTaskCount_tasksOnlyRemovedPartitions() {
        TaskState task1 = generateTaskStateWithPartitions(
                generatePartitions(4, () -> PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.REMOVED)
                        .build()));
        TaskState task2 = generateTaskStateWithPartitions(
                generatePartitions(6, () -> PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.REMOVED)
                        .build()));

        long idlingTaskCount = TaskScalerUtil.idlingTaskCount(createTaskSyncEvent(task1, task2));
        assertThat(idlingTaskCount).isEqualTo(2);
    }

    @Test
    void idlingTaskCount_taskInProgressPartitions() {
        TaskState task1 = generateTaskStateWithPartitions(List.of(
                PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.CREATED)
                        .build(),
                PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.REMOVED)
                        .build()));
        TaskState task2 = generateTaskStateWithPartitions(List.of(
                PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.READY_FOR_STREAMING)
                        .build(),
                PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.FINISHED)
                        .build()));

        long idlingTaskCount = TaskScalerUtil.idlingTaskCount(createTaskSyncEvent(task1, task2));
        assertThat(idlingTaskCount).isZero();
    }

    @Test
    void idlingTaskCountTest() {
        TaskState task1 = generateTaskStateWithPartitions(List.of(
                PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.RUNNING)
                        .build(),
                PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.REMOVED)
                        .build()));
        TaskState task2 = generateTaskStateWithPartitions(List.of(
                PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.READY_FOR_STREAMING)
                        .build(),
                PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.CREATED)
                        .build()));
        TaskState task3 = generateTaskStateWithPartitions(List.of());
        TaskState task4 = generateTaskStateWithPartitions(List.of(
                PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.FINISHED)
                        .build(),
                PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.REMOVED)
                        .build()));
        TaskState task5 = generateTaskStateWithPartitions(List.of(
                PartitionState
                        .builder()
                        .token(UUID.randomUUID().toString())
                        .state(PartitionStateEnum.REMOVED)
                        .build()));

        long idlingTaskCount = TaskScalerUtil.idlingTaskCount(createTaskSyncEvent(task1, task2, task3, task4, task5));
        assertThat(idlingTaskCount).isEqualTo(3);
    }

    @Test
    void idlingTaskCount_nullSyncEventTest() {
        long idlingTaskCount = TaskScalerUtil.idlingTaskCount(null);
        assertThat(idlingTaskCount).isZero();
    }
}
