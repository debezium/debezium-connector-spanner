/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.leader;

import static io.debezium.connector.spanner.task.TaskTestHelper.generatePartitions;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.task.PartitionFactory;
import io.debezium.connector.spanner.task.TaskSyncContext;
import io.debezium.connector.spanner.task.TaskSyncContextHolder;
import io.debezium.connector.spanner.task.TaskTestHelper;
import io.debezium.pipeline.ErrorHandler;

@ExtendWith(MockitoExtension.class)
class LeaderActionServiceTest {
    private static final String TASK_UID = "leader-007";

    @Mock
    private MetricsEventPublisher metricsEventPublisher;

    private TaskSyncContextHolder taskSyncContextHolder;
    private LeaderService leaderService;

    @BeforeEach
    void init() {
        taskSyncContextHolder = new TaskSyncContextHolder(metricsEventPublisher);
        taskSyncContextHolder.init(TaskSyncContext.builder().taskUid(TASK_UID)
                .rebalanceState(RebalanceState.START_INITIAL_SYNC)
                .build());

        SpannerConnectorConfig spannerConnectorConfig = Mockito.mock(SpannerConnectorConfig.class);

        Mockito.when(spannerConnectorConfig.startTime()).thenReturn(Timestamp.now());
        Mockito.when(spannerConnectorConfig.getAwaitTaskAnswerTimeout()).thenReturn(Duration.of(120000, ChronoUnit.MILLIS));

        leaderService = new LeaderService(taskSyncContextHolder,
                spannerConnectorConfig,
                event -> {
                },
                Mockito.mock(ErrorHandler.class),
                Mockito.mock(PartitionFactory.class),
                Mockito.mock(MetricsEventPublisher.class));
    }

    @Test
    void isStartFromScratch_false_leaderInProgress() {
        TaskState taskState1 = generateTaskState("t1", 2, 1, PartitionStateEnum.FINISHED, PartitionStateEnum.REMOVED);
        TaskState taskState2 = generateTaskState("t2", 2, 1, PartitionStateEnum.REMOVED, PartitionStateEnum.FINISHED);
        TaskState leaderState = generateTaskState(TASK_UID, 1, 4, PartitionStateEnum.CREATED, PartitionStateEnum.CREATED);

        taskSyncContextHolder
                .updateAndGet(c -> c.toBuilder().currentTaskState(leaderState).taskStates(TaskTestHelper.createTaskStateMap(taskState1, taskState2)).build());

        boolean startFromScratch = leaderService.isStartFromScratch();

        assertThat(startFromScratch).isFalse();
    }

    @Test
    void isStartFromScratch_false_someTaskInProgress() {
        TaskState taskState1 = generateTaskState("t1", 2, 1, PartitionStateEnum.FINISHED, PartitionStateEnum.REMOVED);
        TaskState taskState2 = generateTaskState("t2", 2, 1, PartitionStateEnum.REMOVED, PartitionStateEnum.READY_FOR_STREAMING);
        TaskState leaderState = generateTaskState(TASK_UID, 1, 4, PartitionStateEnum.FINISHED, PartitionStateEnum.FINISHED);

        taskSyncContextHolder
                .updateAndGet(c -> c.toBuilder().currentTaskState(leaderState).taskStates(TaskTestHelper.createTaskStateMap(taskState1, taskState2)).build());

        boolean startFromScratch = leaderService.isStartFromScratch();

        assertThat(startFromScratch).isFalse();
    }

    @Test
    void isStartFromScratch_true_someTaskInProgress() {
        TaskState taskState1 = generateTaskState("t1", 2, 1, PartitionStateEnum.FINISHED, PartitionStateEnum.REMOVED);
        TaskState taskState2 = generateTaskState("t2", 2, 1, PartitionStateEnum.REMOVED, PartitionStateEnum.REMOVED);
        TaskState leaderState = generateTaskState(TASK_UID, 1, 4, PartitionStateEnum.FINISHED, PartitionStateEnum.FINISHED);

        taskSyncContextHolder
                .updateAndGet(c -> c.toBuilder().currentTaskState(leaderState).taskStates(TaskTestHelper.createTaskStateMap(taskState1, taskState2)).build());

        boolean startFromScratch = leaderService.isStartFromScratch();

        assertThat(startFromScratch).isTrue();
    }

    @Test
    @Timeout(10)
    void awaitAllNewTaskStateUpdatesWorks() throws InterruptedException {
        String consumer0 = "consumer0";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";

        TaskState leaderState = TaskState.builder().taskUid(consumer1.toUpperCase()).consumerId(consumer1).build();

        taskSyncContextHolder.updateAndGet(context -> context.toBuilder().consumerId(consumer1).build());

        taskSyncContextHolder.updateAndGet(c -> c.toBuilder().currentTaskState(leaderState).taskStates(Map.of()).build());

        Set<String> consumers = Set.of(consumer1, consumer2, consumer3);
        Set<String> consumersWithoutLeader = Set.of(consumer2, consumer3);
        BlockingQueue<String> queue = new ArrayBlockingQueue<>(consumers.size(), false, consumersWithoutLeader);

        Thread populator = new Thread(() -> {
            try {
                while (!queue.isEmpty()) {
                    Thread.sleep(40);

                    String consumer = queue.take();

                    TaskState newTask = TaskState.builder().taskUid(consumer.toUpperCase()).consumerId(consumer).build();
                    Map<String, TaskState> newTaskStates = new HashMap<>(taskSyncContextHolder.get().getTaskStates());
                    newTaskStates.put(newTask.getTaskUid(), newTask);

                    taskSyncContextHolder.updateAndGet(c -> c.toBuilder().taskStates(newTaskStates).build());
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        populator.start();

        Map<String, String> result = leaderService.awaitAllNewTaskStateUpdates(consumers, taskSyncContextHolder.get().getRebalanceGenerationId());

        assertThat(result).hasSameSizeAs(consumers);
        assertThat(result).doesNotContainKey(consumer0);
        assertThat(result.get(consumer1)).isEqualTo(consumer1.toUpperCase());
        assertThat(result.get(consumer2)).isEqualTo(consumer2.toUpperCase());
        assertThat(result.get(consumer3)).isEqualTo(consumer3.toUpperCase());
    }

    private TaskState generateTaskState(String taskUid, int numPartitions, int numSharedPartitions, PartitionStateEnum partitionStatus,
                                        PartitionStateEnum sharedPartitionStatus) {
        List<PartitionState> partitions = generatePartitions(numPartitions,
                () -> PartitionState.builder().token(UUID.randomUUID().toString()).state(partitionStatus).build());
        List<PartitionState> sharedPartitions = generatePartitions(numSharedPartitions,
                () -> PartitionState.builder().token(UUID.randomUUID().toString()).state(sharedPartitionStatus).build());
        return TaskState.builder().taskUid(taskUid).partitions(partitions).sharedPartitions(sharedPartitions).build();
    }
}
