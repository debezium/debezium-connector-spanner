/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.scaler;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TaskScaleCalculatorTest {

    @Test
    void newTasksCount_setInitialTasksCountEqualsToTasksMinTest() {
        int tasksMin = 2;
        int newTasksCount = TaskScaleCalculator.newTasksCount(0, 2, 20, 2, 0, 0);
        assertThat(newTasksCount).isEqualTo(tasksMin);
    }

    @Test
    void newTasksCount_initialScaleOutTest() {
        int partitionsInWorkCount = 10;
        int desiredPartitionsTasks = 2;

        int newTasksCount = TaskScaleCalculator.newTasksCount(0, desiredPartitionsTasks, 20, 2, partitionsInWorkCount, 0);
        assertThat(newTasksCount).isEqualTo(partitionsInWorkCount / desiredPartitionsTasks);
    }

    @Test
    void newTasksCount_noScaleRequiredTest() {
        int currentTasksCount = 5;
        int newTasksCount = TaskScaleCalculator.newTasksCount(currentTasksCount, 2, 20, 2, 10, 0);
        assertThat(newTasksCount).isEqualTo(currentTasksCount);
    }

    @Test
    void newTasksCount_scaleOutTest() {
        int currentTasksCount = 5;
        int newTasksCount = TaskScaleCalculator.newTasksCount(currentTasksCount, 2, 20, 2, 11, 0);
        assertThat(newTasksCount).isGreaterThan(currentTasksCount);
    }

    @Test
    void newTasksCount_scaleOutToTasksMaxTest() {
        int currentTasksCount = 5;
        int tasksMax = 100;
        int newTasksCount = TaskScaleCalculator.newTasksCount(currentTasksCount, 1, tasksMax, 2, 300, 0);
        assertThat(newTasksCount).isGreaterThan(currentTasksCount).isEqualTo(tasksMax);
    }

    @Test
    void newTasksCount_cannotScaleOutAfterReachingTasksMaxTest() {
        int currentTasksCount = 5;
        int newTasksCount = TaskScaleCalculator.newTasksCount(currentTasksCount, 2, currentTasksCount, 2, 11, 0);
        assertThat(newTasksCount).isEqualTo(currentTasksCount);
    }

    @Test
    void newTasksCount_scaleInTest() {
        int currentTasksCount = 20;
        int newTasksCount = TaskScaleCalculator.newTasksCount(currentTasksCount, 2, 20, 2, 8, 12);
        assertThat(newTasksCount).isLessThan(currentTasksCount);
    }

    @Test
    void newTasksCount_scaleInToTasksMinTest() {
        int currentTasksCount = 20;
        int tasksMin = 12;
        int newTasksCount = TaskScaleCalculator.newTasksCount(currentTasksCount, 2, 20, tasksMin, 8, 12);
        assertThat(newTasksCount).isLessThan(currentTasksCount).isEqualTo(tasksMin);
    }

    @Test
    void newTasksCount_noScaleInRequiredTest() {
        int currentTasksCount = 10;
        int newTasksCount = TaskScaleCalculator.newTasksCount(currentTasksCount, 2, 20, 2, 8, 2);
        assertThat(newTasksCount).isEqualTo(currentTasksCount);
    }
}
