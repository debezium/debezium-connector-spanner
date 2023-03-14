/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static io.debezium.connector.spanner.kafka.internal.BufferedPublisher.mergeTaskSyncEvent;
import static io.debezium.connector.spanner.task.TaskTestHelper.createTaskSyncEvent;
import static io.debezium.connector.spanner.task.TaskTestHelper.generateTaskStateWithPartitions;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

class BufferedPublisherTest {

    @Test
    void testBufferedPublisher_1() {
        AtomicReference<TaskSyncEvent> value = new AtomicReference<TaskSyncEvent>();
        Predicate<TaskSyncEvent> publishImmediately = p -> false;

        Consumer<TaskSyncEvent> onPublish = v -> {
            value.updateAndGet(currentValue -> mergeTaskSyncEvent("task0", currentValue, v));
        };

        BufferedPublisher pub = new BufferedPublisher("task0", "pub-1", 5,
                publishImmediately,
                onPublish);

        pub.start();

        TaskState task0 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder()
                        .token("token1").state(PartitionStateEnum.REMOVED).build(),
                        PartitionState.builder()
                                .token("token2").state(PartitionStateEnum.RUNNING).build()),
                List.of(PartitionState.builder()
                        .token("token3").state(PartitionStateEnum.CREATED).build()));

        TaskState task1 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder()
                        .token("token1").state(PartitionStateEnum.REMOVED).build(),
                        PartitionState.builder()
                                .token("token2").state(PartitionStateEnum.FINISHED).build()),
                List.of(PartitionState.builder()
                        .token("token4").state(PartitionStateEnum.CREATED).build()));

        TaskSyncEvent taskSyncEvent1 = createTaskSyncEvent("task0", "consumer0", 1, MessageTypeEnum.REGULAR, task0);
        pub.buffer(taskSyncEvent1);
        TaskSyncEvent taskSyncEvent2 = createTaskSyncEvent("task0", "consumer1", 2, MessageTypeEnum.REGULAR, task1);
        pub.buffer(taskSyncEvent2);
        pub.publishBuffered();
        pub.close();

        TaskSyncEvent finalTaskSyncEvent = value.get();
        Assertions.assertEquals("task0", finalTaskSyncEvent.getTaskUid());
        Assertions.assertEquals(1, finalTaskSyncEvent.getTaskStates().size());

        TaskState taskState1 = finalTaskSyncEvent.getTaskStates().get(finalTaskSyncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getTaskUid(), "task0");

        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 2);
        PartitionState partition1 = taskState1.getPartitionsMap().get("token1");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.REMOVED);
        PartitionState partition2 = taskState1.getPartitionsMap().get("token2");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.FINISHED);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 2);
        PartitionState partition3 = taskState1.getSharedPartitionsMap().get("token3");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.CREATED);
        PartitionState partition4 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition4.getState(), PartitionStateEnum.CREATED);
    }

    @Test
    void testBufferedPublisher_2() {
        AtomicReference<TaskSyncEvent> value = new AtomicReference<TaskSyncEvent>();
        Predicate<TaskSyncEvent> publishImmediately = p -> false;

        Consumer<TaskSyncEvent> onPublish = v -> {
            value.updateAndGet(currentValue -> mergeTaskSyncEvent("task0", currentValue, v));
        };

        BufferedPublisher pub = new BufferedPublisher("task0", "pub-1", 5,
                publishImmediately,
                onPublish);

        pub.start();

        TaskState task0 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder()
                        .token("token1").state(PartitionStateEnum.REMOVED).build(),
                        PartitionState.builder()
                                .token("token2").state(PartitionStateEnum.RUNNING).build()),
                List.of(PartitionState.builder()
                        .token("token3").state(PartitionStateEnum.CREATED).build()));

        TaskState task1 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder()
                        .token("token1").state(PartitionStateEnum.REMOVED).build(),
                        PartitionState.builder()
                                .token("token2").state(PartitionStateEnum.FINISHED).build()),
                List.of(PartitionState.builder()
                        .token("token4").state(PartitionStateEnum.CREATED).build()));

        // This task state is a separate task state.
        TaskState task2 = generateTaskStateWithPartitions("task1",
                List.of(PartitionState.builder()
                        .token("token5").state(PartitionStateEnum.CREATED).build(),
                        PartitionState.builder()
                                .token("token6").state(PartitionStateEnum.CREATED).build()),
                List.of(PartitionState.builder()
                        .token("token7").state(PartitionStateEnum.RUNNING).build()));

        // This task state is a separate task state.
        TaskState task3 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder()
                        .token("token8").state(PartitionStateEnum.CREATED).build(),
                        PartitionState.builder()
                                .token("token9").state(PartitionStateEnum.CREATED).build()),
                List.of(PartitionState.builder()
                        .token("token10").state(PartitionStateEnum.RUNNING).build()));

        // This task state is a separate task state.
        TaskState task4 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder()
                        .token("token8").state(PartitionStateEnum.SCHEDULED).build(),
                        PartitionState.builder()
                                .token("token9").state(PartitionStateEnum.RUNNING).build()),
                List.of(PartitionState.builder()
                        .token("token10").state(PartitionStateEnum.RUNNING).build()));

        TaskSyncEvent taskSyncEvent1 = createTaskSyncEvent("task0", "consumer0", 1, MessageTypeEnum.REGULAR, task0);
        pub.buffer(taskSyncEvent1);
        TaskSyncEvent taskSyncEvent2 = createTaskSyncEvent("task0", "consumer1", 2, MessageTypeEnum.REGULAR, task1);
        pub.buffer(taskSyncEvent2);
        TaskSyncEvent taskSyncEvent3 = createTaskSyncEvent("task1", "consumer1", 2, MessageTypeEnum.REGULAR, task2);
        pub.buffer(taskSyncEvent3);
        TaskSyncEvent taskSyncEvent4 = createTaskSyncEvent("task0", "consumer1", 2, MessageTypeEnum.REGULAR, task3);
        pub.buffer(taskSyncEvent4);
        TaskSyncEvent taskSyncEvent5 = createTaskSyncEvent("task0", "consumer1", 2, MessageTypeEnum.REGULAR, task4);
        pub.buffer(taskSyncEvent5);
        pub.publishBuffered();
        pub.close();

        // Assertions.assertEquals(value.get(), expectedTaskSyncEvent);
        TaskSyncEvent finalTaskSyncEvent = value.get();
        Assertions.assertEquals("task0", finalTaskSyncEvent.getTaskUid());
        Assertions.assertEquals(1, finalTaskSyncEvent.getTaskStates().size());

        TaskState taskState1 = finalTaskSyncEvent.getTaskStates().get(finalTaskSyncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getTaskUid(), "task0");

        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        PartitionState partition1 = taskState1.getPartitionsMap().get("token1");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.REMOVED);
        PartitionState partition2 = taskState1.getPartitionsMap().get("token2");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.FINISHED);
        PartitionState partition3 = taskState1.getPartitionsMap().get("token8");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.SCHEDULED);
        PartitionState partition4 = taskState1.getPartitionsMap().get("token9");
        Assertions.assertEquals(partition4.getState(), PartitionStateEnum.RUNNING);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 3);
        PartitionState partition5 = taskState1.getSharedPartitionsMap().get("token3");
        Assertions.assertEquals(partition5.getState(), PartitionStateEnum.CREATED);
        PartitionState partition6 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition6.getState(), PartitionStateEnum.CREATED);
        PartitionState partition7 = taskState1.getSharedPartitionsMap().get("token10");
        Assertions.assertEquals(partition7.getState(), PartitionStateEnum.RUNNING);
    }

    @Test
    void testBufferedPublisher_3() {
        AtomicReference<TaskSyncEvent> value = new AtomicReference<TaskSyncEvent>();
        Predicate<TaskSyncEvent> publishImmediately = p -> false;

        Consumer<TaskSyncEvent> onPublish = v -> {
            value.updateAndGet(currentValue -> mergeTaskSyncEvent("task0", currentValue, v));
        };

        BufferedPublisher pub = new BufferedPublisher("task0", "pub-1", 5,
                publishImmediately,
                onPublish);

        pub.start();

        TaskState task0 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder()
                        .token("token1").state(PartitionStateEnum.REMOVED).build(),
                        PartitionState.builder()
                                .token("token2").state(PartitionStateEnum.RUNNING).build()),
                List.of(PartitionState.builder()
                        .token("token3").state(PartitionStateEnum.CREATED).build()));

        TaskState task1 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder()
                        .token("token1").state(PartitionStateEnum.REMOVED).build(),
                        PartitionState.builder()
                                .token("token2").state(PartitionStateEnum.FINISHED).build()),
                List.of(PartitionState.builder()
                        .token("token4").state(PartitionStateEnum.CREATED).build()));

        // This task state is a separate task state.
        TaskState task2 = generateTaskStateWithPartitions("task1",
                List.of(PartitionState.builder()
                        .token("token5").state(PartitionStateEnum.CREATED).build(),
                        PartitionState.builder()
                                .token("token6").state(PartitionStateEnum.CREATED).build()),
                List.of(PartitionState.builder()
                        .token("token7").state(PartitionStateEnum.RUNNING).build()));

        // This task state is a separate task state.
        TaskState task3 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder()
                        .token("token8").state(PartitionStateEnum.CREATED).build(),
                        PartitionState.builder()
                                .token("token9").state(PartitionStateEnum.CREATED).build()),
                List.of(PartitionState.builder()
                        .token("token10").state(PartitionStateEnum.RUNNING).build()));

        // This task state is a separate task state.
        TaskState task4 = generateTaskStateWithPartitions("task0",
                List.of(PartitionState.builder()
                        .token("token8").state(PartitionStateEnum.SCHEDULED).build(),
                        PartitionState.builder()
                                .token("token9").state(PartitionStateEnum.RUNNING).build()),
                List.of(PartitionState.builder()
                        .token("token10").state(PartitionStateEnum.RUNNING).build()));

        TaskSyncEvent taskSyncEvent1 = createTaskSyncEvent("task0", "consumer0", 1, MessageTypeEnum.REGULAR, task0);
        pub.buffer(taskSyncEvent1);
        TaskSyncEvent taskSyncEvent2 = createTaskSyncEvent("task0", "consumer1", 2, MessageTypeEnum.REGULAR, task1);
        pub.buffer(taskSyncEvent2);
        TaskSyncEvent taskSyncEvent3 = createTaskSyncEvent("task0", "consumer1", 2, MessageTypeEnum.REGULAR, task2);
        pub.buffer(taskSyncEvent3);
        TaskSyncEvent taskSyncEvent4 = createTaskSyncEvent("task0", "consumer1", 2, MessageTypeEnum.REGULAR, task3);
        pub.buffer(taskSyncEvent4);

        pub.publishBuffered();
        TaskSyncEvent finalTaskSyncEvent = value.get();
        Assertions.assertEquals("task0", finalTaskSyncEvent.getTaskUid());
        Assertions.assertEquals(1, finalTaskSyncEvent.getTaskStates().size());

        TaskState taskState1 = finalTaskSyncEvent.getTaskStates().get(finalTaskSyncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getTaskUid(), "task0");

        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        PartitionState partition1 = taskState1.getPartitionsMap().get("token1");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.REMOVED);
        PartitionState partition2 = taskState1.getPartitionsMap().get("token2");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.FINISHED);
        PartitionState partition3 = taskState1.getPartitionsMap().get("token8");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.CREATED);
        PartitionState partition4 = taskState1.getPartitionsMap().get("token9");
        Assertions.assertEquals(partition4.getState(), PartitionStateEnum.CREATED);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 3);
        PartitionState partition5 = taskState1.getSharedPartitionsMap().get("token3");
        Assertions.assertEquals(partition5.getState(), PartitionStateEnum.CREATED);
        PartitionState partition6 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition6.getState(), PartitionStateEnum.CREATED);
        PartitionState partition7 = taskState1.getSharedPartitionsMap().get("token10");
        Assertions.assertEquals(partition7.getState(), PartitionStateEnum.RUNNING);

        TaskSyncEvent taskSyncEvent5 = createTaskSyncEvent("task0", "consumer1", 2, MessageTypeEnum.REGULAR, task4);
        pub.buffer(taskSyncEvent5);
        pub.publishBuffered();
        pub.close();

        // Assertions.assertEquals(value.get(), expectedTaskSyncEvent);
        finalTaskSyncEvent = value.get();
        Assertions.assertEquals("task0", finalTaskSyncEvent.getTaskUid());
        Assertions.assertEquals(1, finalTaskSyncEvent.getTaskStates().size());

        taskState1 = finalTaskSyncEvent.getTaskStates().get(finalTaskSyncEvent.getTaskUid());
        Assertions.assertEquals(taskState1.getTaskUid(), "task0");

        Assertions.assertEquals(taskState1.getPartitionsMap().size(), 4);
        partition1 = taskState1.getPartitionsMap().get("token1");
        Assertions.assertEquals(partition1.getState(), PartitionStateEnum.REMOVED);
        partition2 = taskState1.getPartitionsMap().get("token2");
        Assertions.assertEquals(partition2.getState(), PartitionStateEnum.FINISHED);
        partition3 = taskState1.getPartitionsMap().get("token8");
        Assertions.assertEquals(partition3.getState(), PartitionStateEnum.SCHEDULED);
        partition4 = taskState1.getPartitionsMap().get("token9");
        Assertions.assertEquals(partition4.getState(), PartitionStateEnum.RUNNING);

        Assertions.assertEquals(taskState1.getSharedPartitions().size(), 3);
        partition5 = taskState1.getSharedPartitionsMap().get("token3");
        Assertions.assertEquals(partition5.getState(), PartitionStateEnum.CREATED);
        partition6 = taskState1.getSharedPartitionsMap().get("token4");
        Assertions.assertEquals(partition6.getState(), PartitionStateEnum.CREATED);
        partition7 = taskState1.getSharedPartitionsMap().get("token10");
        Assertions.assertEquals(partition7.getState(), PartitionStateEnum.RUNNING);
    }

}
