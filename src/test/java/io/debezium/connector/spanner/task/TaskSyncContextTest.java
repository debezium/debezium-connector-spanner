/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static io.debezium.connector.spanner.task.TaskTestHelper.generateTaskStateWithPartitions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

class TaskSyncContextTest {
    @Test
    void testEmptyTaskSyncContext() {
        TaskSyncContext taskSyncContext = buildEmptyTaskSyncContext();
        TaskState task0 = generateTaskStateWithPartitions(
                "task0", List.of(), List.of());

        TaskSyncEvent syncEvent = taskSyncContext.buildRebalanceAnswerTaskSyncEvent();

        // Build rebalance answer.
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REBALANCE_ANSWER, syncEvent.getMessageType());

        // Build incremental message.
        syncEvent = taskSyncContext.buildIncrementalTaskSyncEvent(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REGULAR, syncEvent.getMessageType());

        // Build epoch update message.
        syncEvent = taskSyncContext.buildUpdateEpochTaskSyncEvent();
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(3, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.UPDATE_EPOCH, syncEvent.getMessageType());

        // Build new epoch message
        syncEvent = taskSyncContext.buildNewEpochTaskSyncEvent();
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(3, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.NEW_EPOCH, syncEvent.getMessageType());
    }

    @Test
    void testPopulatedTaskSyncContext() {
        TaskSyncContext taskSyncContext = buildTaskSyncContextWithPartitions();
        TaskState task0 = generateTaskStateWithPartitions(
                "task0", List.of(), List.of());

        TaskSyncEvent syncEvent = taskSyncContext.buildRebalanceAnswerTaskSyncEvent();

        // Build rebalance answer.
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REBALANCE_ANSWER, syncEvent.getMessageType());
        TaskState taskState1 = syncEvent.getTaskStates().get(syncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 2);

        // Build incremental message.
        List<String> updatedOwnedPartitions = new ArrayList<>();
        List<String> updatedSharedPartitions = new ArrayList<>();
        List<String> removedOwnedPartitions = new ArrayList<>();
        List<String> removedSharedPartitions = new ArrayList<>();
        updatedOwnedPartitions.add("token0");
        updatedOwnedPartitions.add("token2");
        updatedSharedPartitions.add("token4");
        syncEvent = taskSyncContext.buildIncrementalTaskSyncEvent(
                updatedOwnedPartitions, updatedSharedPartitions, removedOwnedPartitions,
                removedSharedPartitions);
        taskState1 = syncEvent.getTaskStates().get(syncEvent.getTaskUid());
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REGULAR, syncEvent.getMessageType());
        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 2);
        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 1);

        // Build epoch update message.
        syncEvent = taskSyncContext.buildUpdateEpochTaskSyncEvent();
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(3, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.UPDATE_EPOCH, syncEvent.getMessageType());

        taskState1 = syncEvent.getTaskStates().get(syncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 2);

        TaskState taskState2 = syncEvent.getTaskStates().get("task1");
        Assertions.assertEquals(taskState2.getPartitionsMap().size(), 2);
        Assertions.assertEquals(taskState2.getSharedPartitions().size(), 1);
        TaskState taskState3 = syncEvent.getTaskStates().get("task2");
        Assertions.assertEquals(taskState3.getPartitionsMap().size(), 2);
        Assertions.assertEquals(taskState3.getSharedPartitions().size(), 1);

        // Build new epoch message
        syncEvent = taskSyncContext.buildNewEpochTaskSyncEvent();
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(3, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.NEW_EPOCH, syncEvent.getMessageType());

        taskState1 = syncEvent.getTaskStates().get(syncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 2);

        taskState2 = syncEvent.getTaskStates().get("task1");
        Assertions.assertEquals(taskState2.getPartitionsMap().size(), 2);
        Assertions.assertEquals(taskState2.getSharedPartitions().size(), 1);
        taskState3 = syncEvent.getTaskStates().get("task2");
        Assertions.assertEquals(taskState3.getPartitionsMap().size(), 2);
        Assertions.assertEquals(taskState3.getSharedPartitions().size(), 1);
    }

    @Test
    void testIncrementalAnswer() {
        TaskSyncContext taskSyncContext = buildTaskSyncContextWithPartitions();

        List<String> updatedOwnedPartitions = new ArrayList<>();
        List<String> updatedSharedPartitions = new ArrayList<>();
        List<String> removedOwnedPartitions = new ArrayList<>();
        List<String> removedSharedPartitions = new ArrayList<>();
        // Test basic newly owned partitions, newly modified, and newly shared partitions.
        updatedOwnedPartitions.add("token0");
        updatedOwnedPartitions.add("token2");
        updatedSharedPartitions.add("token4");
        // Build incremental message.

        TaskSyncEvent syncEvent = taskSyncContext.buildIncrementalTaskSyncEvent(
                updatedOwnedPartitions, updatedSharedPartitions, removedOwnedPartitions, removedSharedPartitions);
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REGULAR, syncEvent.getMessageType());

        TaskState taskState1 = syncEvent.getTaskStates().get(syncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 2);
        PartitionState partition1 = taskState1.getPartitionsMap().get("token0");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.CREATED);
        PartitionState partition2 = taskState1.getPartitionsMap().get("token2");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.RUNNING);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 1);
        PartitionState partition3 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.CREATED);
        updatedOwnedPartitions.clear();
        updatedSharedPartitions.clear();

        // Let's say a partition was owned and then updated.
        updatedOwnedPartitions.add("token2");
        updatedOwnedPartitions.add("token2");
        updatedOwnedPartitions.add("token3");
        updatedSharedPartitions.add("token4");

        syncEvent = taskSyncContext.buildIncrementalTaskSyncEvent(
                updatedOwnedPartitions, updatedSharedPartitions, removedOwnedPartitions,
                removedSharedPartitions);
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REGULAR, syncEvent.getMessageType());

        taskState1 = syncEvent.getTaskStates().get(syncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 2);

        partition1 = taskState1.getPartitionsMap().get("token3");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.FINISHED);
        partition2 = taskState1.getPartitionsMap().get("token2");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.RUNNING);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 1);
        partition3 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.CREATED);
        updatedOwnedPartitions.clear();
        updatedSharedPartitions.clear();

        // Let's say a partition was owned and then removed.
        updatedOwnedPartitions.add("token1");
        updatedOwnedPartitions.add("token3");
        removedOwnedPartitions.add("token1");
        updatedSharedPartitions.add("token4");

        syncEvent = taskSyncContext.buildIncrementalTaskSyncEvent(
                updatedOwnedPartitions, updatedSharedPartitions, removedOwnedPartitions,
                removedSharedPartitions);

        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REGULAR, syncEvent.getMessageType());

        taskState1 = syncEvent.getTaskStates().get(syncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 2);
        partition1 = taskState1.getPartitionsMap().get("token3");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.FINISHED);
        partition2 = taskState1.getPartitionsMap().get("token1");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.REMOVED);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 1);
        partition3 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.CREATED);

        updatedOwnedPartitions.clear();
        removedOwnedPartitions.clear();
        updatedSharedPartitions.clear();

        // Let's say a partition was updated and then removed.
        updatedOwnedPartitions.add("token0");
        updatedOwnedPartitions.add("token1");
        updatedOwnedPartitions.add("token3");
        removedOwnedPartitions.add("token1");
        updatedSharedPartitions.add("token4");

        syncEvent = taskSyncContext.buildIncrementalTaskSyncEvent(
                updatedOwnedPartitions, updatedSharedPartitions, removedOwnedPartitions,
                removedSharedPartitions);

        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REGULAR, syncEvent.getMessageType());

        taskState1 = syncEvent.getTaskStates().get(syncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 3);
        partition1 = taskState1.getPartitionsMap().get("token3");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.FINISHED);
        partition2 = taskState1.getPartitionsMap().get("token1");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.REMOVED);
        partition3 = taskState1.getPartitionsMap().get("token0");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.CREATED);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 1);
        PartitionState partition4 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition4.getState(), PartitionStateEnum.CREATED);

        updatedOwnedPartitions.clear();
        removedOwnedPartitions.clear();
        updatedSharedPartitions.clear();

        // Let's say a partition was shared and then removed.
        updatedSharedPartitions.add("token5");
        removedSharedPartitions.add("token5");
        syncEvent = taskSyncContext.buildIncrementalTaskSyncEvent(
                updatedOwnedPartitions, updatedSharedPartitions, removedOwnedPartitions,
                removedSharedPartitions);

        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REGULAR, syncEvent.getMessageType());

        taskState1 = syncEvent.getTaskStates().get(syncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 0);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 1);
        partition1 = taskState1.getSharedPartitionsMap().get("token5");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.REMOVED);
    }

    private TaskSyncContext buildEmptyTaskSyncContext() {
        return TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder().taskUid("task0")
                        .partitions(List.of())
                        .sharedPartitions(List.of())
                        .build())
                .taskStates(Map.of("task1", TaskState.builder()
                        .taskUid("task1")
                        .partitions(List.of())
                        .sharedPartitions(List.of())
                        .build(),
                        "task2", TaskState.builder()
                                .taskUid("task2")
                                .partitions(List.of())
                                .sharedPartitions(List.of())
                                .build()))
                .build();
    }

    private TaskSyncContext buildTaskSyncContextWithPartitions() {

        TaskState task0 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder().token("token0").state(PartitionStateEnum.CREATED).build(),
                        PartitionState.builder()
                                .token("token1").state(PartitionStateEnum.REMOVED).build(),
                        PartitionState.builder()
                                .token("token2").state(PartitionStateEnum.RUNNING).build(),
                        PartitionState.builder()
                                .token("token3").state(PartitionStateEnum.FINISHED).build()),
                List.of(PartitionState.builder()
                        .token("token4").state(PartitionStateEnum.CREATED).build(),
                        PartitionState.builder()
                                .token("token5").state(PartitionStateEnum.REMOVED).build()));

        TaskState task1 = generateTaskStateWithPartitions("task1",
                List.of(PartitionState.builder()
                        .token("token6").state(PartitionStateEnum.REMOVED).build(),
                        PartitionState.builder()
                                .token("token7").state(PartitionStateEnum.FINISHED).build()),
                List.of(PartitionState.builder()
                        .token("token8").state(PartitionStateEnum.CREATED).build()));

        TaskState task2 = generateTaskStateWithPartitions("task2",
                List.of(PartitionState.builder()
                        .token("token1").state(PartitionStateEnum.CREATED).build(),
                        PartitionState.builder()
                                .token("token2").state(PartitionStateEnum.REMOVED).build()),
                List.of(PartitionState.builder()
                        .token("token3").state(PartitionStateEnum.RUNNING).build()));

        return TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(task0)
                .taskStates(Map.of("task1", task1,
                        "task2", task2))
                .build();
    }
}