/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static io.debezium.connector.spanner.task.TaskTestHelper.generateTaskStateWithPartitions;

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

        TaskSyncEvent syncEvent = taskSyncContext.buildRebalanceAnswerTaskSyncEvent(0);

        // Build rebalance answer.
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REBALANCE_ANSWER, syncEvent.getMessageType());

        // Build incremental message.
        syncEvent = taskSyncContext.buildCurrentTaskSyncEvent();
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

        TaskSyncEvent syncEvent = taskSyncContext.buildRebalanceAnswerTaskSyncEvent(0);

        // Build rebalance answer.
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REBALANCE_ANSWER, syncEvent.getMessageType());
        TaskState taskState1 = syncEvent.getTaskStates().get(syncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 2);

        // Build incremental message.
        syncEvent = taskSyncContext.buildCurrentTaskSyncEvent();
        Assertions.assertEquals("task0", syncEvent.getTaskUid());
        Assertions.assertEquals(1, syncEvent.getTaskStates().size());
        Assertions.assertEquals(MessageTypeEnum.REGULAR, syncEvent.getMessageType());
        taskState1 = syncEvent.getTaskStates().get(syncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 2);

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
