/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

class CheckPartitionDuplicationOperationTest {

    @Test
    void doOperation() {
        ChangeStream changeStream = Mockito.mock(ChangeStream.class);

        CheckPartitionDuplicationOperation checkPartitionDuplicationOperation = new CheckPartitionDuplicationOperation(changeStream);

        TaskSyncContext taskSyncContext = TaskSyncContext.builder()
                .taskUid("taskO")
                .currentTaskState(TaskState.builder().taskUid("taskO")
                        .partitions(List.of(PartitionState.builder().token("t1").state(PartitionStateEnum.RUNNING).build(),
                                PartitionState.builder().token("t2").state(PartitionStateEnum.RUNNING).build()))
                        .build())
                .taskStates(Map.of("task1", TaskState.builder()
                        .taskUid("task1")
                        .partitions(List.of(PartitionState.builder().token("t1").state(PartitionStateEnum.RUNNING).build())).build()))
                .taskStates(Map.of("task2", TaskState.builder()
                        .taskUid("task2")
                        .partitions(List.of(PartitionState.builder().token("t1").state(PartitionStateEnum.RUNNING).build())).build()))
                .build();

        checkPartitionDuplicationOperation.doOperation(taskSyncContext);

        Assertions.assertTrue(checkPartitionDuplicationOperation.isRequiredPublishSyncEvent());

        Mockito.verify(changeStream, Mockito.times(1)).stop(Mockito.anyString());

    }
}
