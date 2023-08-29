/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static io.debezium.connector.spanner.task.SyncEventMerger.mergeEpochUpdate;
import static io.debezium.connector.spanner.task.SyncEventMerger.mergeIncrementalTaskSyncEvent;
import static io.debezium.connector.spanner.task.SyncEventMerger.mergeNewEpoch;
import static io.debezium.connector.spanner.task.SyncEventMerger.mergeRebalanceAnswer;
import static io.debezium.connector.spanner.task.TaskTestHelper.generateTaskStateWithPartitions;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

class SyncEventMergerTest {
    @Test
    void testMergeRebalanceAnswer() {
        TaskSyncContext taskSyncContext1 = buildTaskSyncContext1();
        TaskSyncContext taskSyncContext2 = buildTaskSyncContext2();

        TaskSyncEvent rebalanceAnswer1 = taskSyncContext1.buildRebalanceAnswerTaskSyncEvent();
        TaskSyncContext mergedRebalanceAnswer = mergeRebalanceAnswer(
                taskSyncContext2, rebalanceAnswer1);

        Assertions.assertEquals("task2", mergedRebalanceAnswer.getTaskUid());
        Assertions.assertEquals(2, mergedRebalanceAnswer.getTaskStates().size());

        TaskState taskState1 = mergedRebalanceAnswer.getTaskStates().get("task0");
        Assertions.assertEquals(taskState1.getTaskUid(), "task0");

        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        PartitionState partition1 = taskState1.getPartitionsMap().get("token0");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.CREATED);
        PartitionState partition2 = taskState1.getPartitionsMap().get("token1");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.REMOVED);
        PartitionState partition3 = taskState1.getPartitionsMap().get("token2");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.RUNNING);
        PartitionState partition4 = taskState1.getPartitionsMap().get("token3");
        Assertions.assertEquals(partition4.getState(), PartitionStateEnum.FINISHED);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 2);
        PartitionState partition5 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition5.getState(), PartitionStateEnum.CREATED);
        PartitionState partition6 = taskState1.getSharedPartitionsMap().get("token5");
        Assertions.assertEquals(partition6.getState(), PartitionStateEnum.REMOVED);

        TaskState taskState2 = mergedRebalanceAnswer.getCurrentTaskState();
        Assertions.assertEquals(taskState2.getTaskUid(), "task2");
        Assertions.assertEquals(taskState2.getPartitionsMap().size(), 4);
        Assertions.assertEquals(taskState2.getSharedPartitionsMap().size(), 1);

        TaskState taskState3 = mergedRebalanceAnswer.getTaskStates().get("task1");
        Assertions.assertEquals(taskState3.getTaskUid(), "task1");
        Assertions.assertEquals(taskState3.getPartitionsMap().size(), 2);
        Assertions.assertEquals(taskState3.getSharedPartitionsMap().size(), 1);
    }

    void testMergeIncrementalAnswer() {
        TaskSyncContext taskSyncContext1 = buildTaskSyncContext1();
        TaskSyncContext taskSyncContext2 = buildTaskSyncContext2();

        TaskSyncEvent incrementalAnswer1 = taskSyncContext1.buildCurrentTaskSyncEvent();
        TaskSyncContext mergeIncrementalAnswer = mergeIncrementalTaskSyncEvent(
                taskSyncContext2, incrementalAnswer1);

        Assertions.assertEquals("task2", mergeIncrementalAnswer.getTaskUid());
        Assertions.assertEquals(2, mergeIncrementalAnswer.getTaskStates().size());

        TaskState taskState1 = mergeIncrementalAnswer.getTaskStates().get("task0");
        Assertions.assertEquals(taskState1.getTaskUid(), "task0");

        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        PartitionState partition1 = taskState1.getPartitionsMap().get("token0");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.CREATED);
        PartitionState partition2 = taskState1.getPartitionsMap().get("token1");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.REMOVED);
        PartitionState partition3 = taskState1.getPartitionsMap().get("token2");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.RUNNING);
        PartitionState partition4 = taskState1.getPartitionsMap().get("token3");
        Assertions.assertEquals(partition4.getState(), PartitionStateEnum.FINISHED);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 2);
        PartitionState partition5 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition5.getState(), PartitionStateEnum.CREATED);
        PartitionState partition6 = taskState1.getSharedPartitionsMap().get("token5");
        Assertions.assertEquals(partition6.getState(), PartitionStateEnum.REMOVED);

        TaskState taskState2 = mergeIncrementalAnswer.getCurrentTaskState();
        Assertions.assertEquals(taskState2.getTaskUid(), "task2");
        Assertions.assertEquals(taskState2.getPartitionsMap().size(), 4);
        Assertions.assertEquals(taskState2.getSharedPartitionsMap().size(), 1);

        TaskState taskState3 = mergeIncrementalAnswer.getTaskStates().get("task1");
        Assertions.assertEquals(taskState3.getTaskUid(), "task1");
        Assertions.assertEquals(taskState3.getPartitionsMap().size(), 2);
        Assertions.assertEquals(taskState3.getSharedPartitionsMap().size(), 1);
    }

    @Test
    void testMergeNewEpoch() {
        TaskSyncContext taskSyncContext1 = buildTaskSyncContext1();
        TaskSyncContext taskSyncContext2 = buildTaskSyncContext2();

        TaskSyncEvent syncEvent = taskSyncContext1.buildNewEpochTaskSyncEvent();
        TaskSyncContext mergeNewEpoch = mergeNewEpoch(
                taskSyncContext2, syncEvent);

        Assertions.assertEquals("task2", mergeNewEpoch.getTaskUid());
        Assertions.assertEquals(2, mergeNewEpoch.getTaskStates().size());

        Assertions.assertEquals("task2", mergeNewEpoch.getTaskUid());
        Assertions.assertEquals(2, mergeNewEpoch.getTaskStates().size());

        TaskState taskState1 = mergeNewEpoch.getTaskStates().get("task0");
        Assertions.assertEquals(taskState1.getTaskUid(), "task0");

        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        PartitionState partition1 = taskState1.getPartitionsMap().get("token0");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.CREATED);
        PartitionState partition2 = taskState1.getPartitionsMap().get("token1");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.REMOVED);
        PartitionState partition3 = taskState1.getPartitionsMap().get("token2");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.RUNNING);
        PartitionState partition4 = taskState1.getPartitionsMap().get("token3");
        Assertions.assertEquals(partition4.getState(), PartitionStateEnum.FINISHED);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 2);
        PartitionState partition5 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition5.getState(), PartitionStateEnum.CREATED);
        PartitionState partition6 = taskState1.getSharedPartitionsMap().get("token5");
        Assertions.assertEquals(partition6.getState(), PartitionStateEnum.REMOVED);

        TaskState taskState2 = mergeNewEpoch.getCurrentTaskState();
        Assertions.assertEquals(taskState2.getTaskUid(), "task2");
        Assertions.assertEquals(taskState2.getPartitionsMap().size(), 4);
        Assertions.assertEquals(taskState2.getSharedPartitionsMap().size(), 1);

        TaskState taskState3 = mergeNewEpoch.getTaskStates().get("task1");
        Assertions.assertEquals(taskState3.getTaskUid(), "task1");
        Assertions.assertEquals(taskState3.getPartitionsMap().size(), 1);
        Assertions.assertEquals(taskState3.getSharedPartitionsMap().size(), 1);
    }

    @Test
    void testMergeEpochupdate() {
        TaskSyncContext taskSyncContext1 = buildTaskSyncContext1();
        TaskSyncContext taskSyncContext2 = buildTaskSyncContext2();

        TaskSyncEvent syncEvent = taskSyncContext1.buildUpdateEpochTaskSyncEvent();
        TaskSyncContext mergedEpochUpdate = mergeEpochUpdate(
                taskSyncContext2, syncEvent);

        Assertions.assertEquals("task2", mergedEpochUpdate.getTaskUid());
        Assertions.assertEquals(2, mergedEpochUpdate.getTaskStates().size());

        Assertions.assertEquals("task2", mergedEpochUpdate.getTaskUid());
        Assertions.assertEquals(2, mergedEpochUpdate.getTaskStates().size());

        TaskState taskState1 = mergedEpochUpdate.getTaskStates().get("task0");
        Assertions.assertEquals(taskState1.getTaskUid(), "task0");

        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        PartitionState partition1 = taskState1.getPartitionsMap().get("token0");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.CREATED);
        PartitionState partition2 = taskState1.getPartitionsMap().get("token1");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.REMOVED);
        PartitionState partition3 = taskState1.getPartitionsMap().get("token2");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.RUNNING);
        PartitionState partition4 = taskState1.getPartitionsMap().get("token3");
        Assertions.assertEquals(partition4.getState(), PartitionStateEnum.FINISHED);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 2);
        PartitionState partition5 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition5.getState(), PartitionStateEnum.CREATED);
        PartitionState partition6 = taskState1.getSharedPartitionsMap().get("token5");
        Assertions.assertEquals(partition6.getState(), PartitionStateEnum.REMOVED);

        TaskState taskState2 = mergedEpochUpdate.getCurrentTaskState();
        Assertions.assertEquals(taskState2.getTaskUid(), "task2");
        Assertions.assertEquals(taskState2.getPartitionsMap().size(), 4);
        Assertions.assertEquals(taskState2.getSharedPartitionsMap().size(), 1);

        TaskState taskState3 = mergedEpochUpdate.getTaskStates().get("task1");
        Assertions.assertEquals(taskState3.getTaskUid(), "task1");
        Assertions.assertEquals(taskState3.getPartitionsMap().size(), 2);
        Assertions.assertEquals(taskState3.getSharedPartitionsMap().size(), 1);
    }

    private TaskSyncContext buildTaskSyncContext1() {

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
                        .token("token6").state(PartitionStateEnum.REMOVED).build()),
                List.of(PartitionState.builder()
                        .token("token8").state(PartitionStateEnum.CREATED).build()));

        TaskState task2 = generateTaskStateWithPartitions("task2",
                List.of(PartitionState.builder()
                        .token("token10").state(PartitionStateEnum.RUNNING).build()),
                List.of(PartitionState.builder()
                        .token("token14").state(PartitionStateEnum.CREATED).build()));

        return TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(task0)
                .taskStates(Map.of("task1", task1,
                        "task2", task2))
                .build();
    }

    private TaskSyncContext buildTaskSyncContext2() {

        TaskState task0 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder().token("token0").state(PartitionStateEnum.CREATED).build()),
                List.of(PartitionState.builder()
                        .token("token4").state(PartitionStateEnum.CREATED).build()));

        TaskState task1 = generateTaskStateWithPartitions("task1",
                List.of(PartitionState.builder()
                        .token("token6").state(PartitionStateEnum.REMOVED).build(),
                        PartitionState.builder()
                                .token("token7").state(PartitionStateEnum.FINISHED).build()),
                List.of(PartitionState.builder()
                        .token("token8").state(PartitionStateEnum.CREATED).build()));

        TaskState task2 = generateTaskStateWithPartitions("task2",
                List.of(PartitionState.builder()
                        .token("token9").state(PartitionStateEnum.CREATED).build(),
                        PartitionState.builder()
                                .token("token10").state(PartitionStateEnum.RUNNING).build(),
                        PartitionState.builder()
                                .token("token11").state(PartitionStateEnum.REMOVED).build(),
                        PartitionState.builder()
                                .token("token12").state(PartitionStateEnum.FINISHED).build()),
                List.of(PartitionState.builder()
                        .token("token14").state(PartitionStateEnum.CREATED).build()));

        return TaskSyncContext.builder()
                .taskUid("task2")
                .currentTaskState(task2)
                .taskStates(Map.of("task1", task1,
                        "task0", task0))
                .build();
    }

}
