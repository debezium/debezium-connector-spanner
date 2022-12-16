/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.leader.rebalancer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;

class TaskPartitionEqualSharingRebalancerTest {

    @Test
    // TODO: check
    @Disabled("Test is randomly failing")
    void rebalance() {
        TaskPartitionEqualSharingRebalancer taskPartitionEqualSharingRebalancer = new TaskPartitionEqualSharingRebalancer();

        TaskState newLeaderTaskState = taskPartitionEqualSharingRebalancer.rebalance(makeLeaderTaskState(),
                Map.of("task1", makeTask1State(),
                        "task2", makeTask2State()),
                Map.of("task3", makeTask3State(),
                        "task4", makeTask4State()));

        Assertions.assertTrue(newLeaderTaskState.getPartitions().size() >= 6);
        Assertions.assertTrue(newLeaderTaskState.getPartitions().size() <= 7);

        Assertions.assertEquals(Optional.of("testToken9"), newLeaderTaskState.getPartitions().stream()
                .filter(partitionState -> partitionState.getState().equals(PartitionStateEnum.FINISHED))
                .map(PartitionState::getToken).findFirst());

        Assertions.assertEquals(0, newLeaderTaskState.getSharedPartitions().stream()
                .filter(partitionState -> !partitionState.getState().equals(PartitionStateEnum.CREATED)).count());

        List<String> actualPartitions = Stream.concat(newLeaderTaskState.getPartitions().stream().map(PartitionState::getToken),
                newLeaderTaskState.getSharedPartitions().stream().map(PartitionState::getToken)).collect(Collectors.toList());

        Set<String> expectedPartitions = Set.of("testToken1", "testToken2", "testToken5", "testToken11", "testToken12", "testToken13",
                "testToken7", "testToken10", "testToken6", "testToken8", "testToken9", "testToken4", "testToken14", "testToken15");

        Assertions.assertEquals(expectedPartitions.size(), actualPartitions.size());

        Assertions.assertEquals(Optional.of("testToken4"), newLeaderTaskState.getPartitions().stream()
                .filter(partitionState -> partitionState.getState().equals(PartitionStateEnum.CREATED))
                .map(PartitionState::getToken).filter(token -> token.equals("testToken4")).findFirst());

        Assertions.assertEquals(Optional.of("testToken2"), newLeaderTaskState.getPartitions().stream()
                .filter(partitionState -> partitionState.getState().equals(PartitionStateEnum.CREATED))
                .map(PartitionState::getToken).filter(token -> token.equals("testToken2")).findFirst());
    }

    private TaskState makeLeaderTaskState() {
        return TaskState.builder()
                .taskUid("leaderTask")
                .partitions(List.of(PartitionState.builder().state(PartitionStateEnum.RUNNING)
                        .assigneeTaskUid("leaderTask")
                        .token("testToken1").build()))
                .sharedPartitions(List.of(PartitionState.builder().state(PartitionStateEnum.CREATED)
                        .assigneeTaskUid("task4")
                        .token("testToken2").build()))
                .build();
    }

    private TaskState makeTask1State() {
        return TaskState.builder()
                .taskUid("task1")
                .partitions(List.of(PartitionState.builder().state(PartitionStateEnum.RUNNING)
                        .assigneeTaskUid("task1")
                        .token("testToken1").build()))
                .sharedPartitions(List.of(PartitionState.builder().state(PartitionStateEnum.CREATED)
                        .assigneeTaskUid("task2")
                        .token("testToken2").build()))
                .build();
    }

    private TaskState makeTask2State() {
        return TaskState.builder()
                .taskUid("task2")
                .partitions(List.of(PartitionState.builder().state(PartitionStateEnum.RUNNING)
                        .assigneeTaskUid("task2")
                        .token("testToken3").build()))
                .sharedPartitions(List.of(PartitionState.builder().state(PartitionStateEnum.CREATED)
                        .assigneeTaskUid("task4")
                        .token("testToken4").build()))
                .build();
    }

    private TaskState makeTask3State() {
        return TaskState.builder()
                .taskUid("task3")
                .partitions(List.of(PartitionState.builder().state(PartitionStateEnum.RUNNING)
                        .assigneeTaskUid("task3")
                        .token("testToken5").build(),
                        PartitionState.builder().state(PartitionStateEnum.RUNNING)
                                .assigneeTaskUid("task3")
                                .token("testToken11").build(),
                        PartitionState.builder().state(PartitionStateEnum.RUNNING)
                                .assigneeTaskUid("task3")
                                .token("testToken12").build(),
                        PartitionState.builder().state(PartitionStateEnum.RUNNING)
                                .assigneeTaskUid("task3")
                                .token("testToken13").build(),
                        PartitionState.builder().state(PartitionStateEnum.RUNNING)
                                .assigneeTaskUid("task3")
                                .token("testToken14").build(),
                        PartitionState.builder().state(PartitionStateEnum.RUNNING)
                                .assigneeTaskUid("task3")
                                .token("testToken15").build()))
                .sharedPartitions(List.of(PartitionState.builder().state(PartitionStateEnum.CREATED)
                        .assigneeTaskUid("task4")
                        .token("testToken6").build()))
                .build();
    }

    private TaskState makeTask4State() {
        return TaskState.builder()
                .taskUid("task4")
                .partitions(List.of(PartitionState.builder().state(PartitionStateEnum.RUNNING)
                        .assigneeTaskUid("task4")
                        .token("testToken7").build(),
                        PartitionState.builder().state(PartitionStateEnum.RUNNING)
                                .assigneeTaskUid("task4")
                                .token("testToken10").build(),
                        PartitionState.builder().state(PartitionStateEnum.RUNNING)
                                .assigneeTaskUid("task4")
                                .token("testToken11").build(),
                        PartitionState.builder().state(PartitionStateEnum.FINISHED)
                                .assigneeTaskUid("task4")
                                .token("testToken9").build()))
                .sharedPartitions(List.of(PartitionState.builder().state(PartitionStateEnum.CREATED)
                        .assigneeTaskUid("task1")
                        .token("testToken8").build()))
                .build();
    }

}
