/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Collection;
import java.util.Map;

import io.debezium.connector.spanner.kafka.internal.model.TaskState;

/**
 * Utility for grouping and filtering tasks,
 * which survived and not after the Rebalance Event
 */
public class TaskStateUtil {
    private TaskStateUtil() {
    }

    public static Map<String, TaskState> filterSurvivedTasksStates(Map<String, TaskState> taskStates, Collection<String> survivedTasksUids) {
        return taskStates
                .entrySet()
                .stream()
                .filter(e -> survivedTasksUids.contains(e.getKey()))
                .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<Boolean, Map<String, TaskState>> splitSurvivedAndObsoleteTaskStates(Map<String, TaskState> taskStates, Collection<String> survivedTasksUids) {
        return taskStates
                .entrySet()
                .stream()
                .collect(partitioningBy(
                        e -> survivedTasksUids.contains(e.getKey()),
                        toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)));
    }
}
