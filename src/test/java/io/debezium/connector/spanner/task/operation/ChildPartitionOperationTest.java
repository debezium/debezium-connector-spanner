/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

class ChildPartitionOperationTest {

    @Test
    void doOperation() {

        TaskSyncContext taskSyncContext = new ChildPartitionOperation(List.of(buildPartition("n1"), buildPartition("n2"))).doOperation(buildTaskSyncContext());

        Assertions.assertEquals(3, taskSyncContext.getCurrentTaskState().getPartitions().size());

        Assertions.assertEquals(2, taskSyncContext.getCurrentTaskState().getSharedPartitions().size());

        taskSyncContext = new ChildPartitionOperation(List.of(buildPartition("n3"), buildPartition("n4"))).doOperation(taskSyncContext);

        Assertions.assertEquals(3, taskSyncContext.getCurrentTaskState().getPartitions().size());

        Assertions.assertEquals(4, taskSyncContext.getCurrentTaskState().getSharedPartitions().size());

        taskSyncContext = new ChildPartitionOperation(List.of(buildPartition("n5"))).doOperation(taskSyncContext);

        Assertions.assertEquals(4, taskSyncContext.getCurrentTaskState().getPartitions().size());

        Assertions.assertEquals(4, taskSyncContext.getCurrentTaskState().getSharedPartitions().size());
    }

    private TaskSyncContext buildTaskSyncContext() {
        return TaskSyncContext.builder()
                .taskUid("taskO")
                .currentTaskState(TaskState.builder().taskUid("taskO")
                        .partitions(List.of(PartitionState.builder().token("t1").state(PartitionStateEnum.RUNNING).build(),
                                PartitionState.builder().token("t2").state(PartitionStateEnum.RUNNING).build(),
                                PartitionState.builder().token("t3").state(PartitionStateEnum.RUNNING).build()))
                        .sharedPartitions(List.of())
                        .build())
                .taskStates(Map.of("task1", TaskState.builder()
                        .taskUid("task1")
                        .partitions(List.of(PartitionState.builder().token("t6").state(PartitionStateEnum.RUNNING).build()))
                        .sharedPartitions(List.of())
                        .build(),
                        "task2", TaskState.builder()
                                .taskUid("task2")
                                .partitions(List.of(PartitionState.builder().token("t7").state(PartitionStateEnum.RUNNING).build()))
                                .sharedPartitions(List.of())
                                .build()))
                .build();
    }

    private Partition buildPartition(String token) {
        return Partition.builder().token(token).parentTokens(Set.of()).startTimestamp(Timestamp.now()).endTimestamp(null).build();
    }
}
