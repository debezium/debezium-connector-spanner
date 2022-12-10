/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import java.util.UUID;

/**
 * Utility to generate unique connector task identifiers
 */
public class TaskUid {
    private TaskUid() {
    }

    public static String generateTaskUid(String connectorName, String taskId) {
        return connectorName + "_task-" + taskId + "_" + UUID.randomUUID();
    }
}
