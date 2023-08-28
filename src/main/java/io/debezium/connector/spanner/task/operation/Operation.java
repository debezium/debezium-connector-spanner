/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Provides an interface for actions which
 * should be done after task state was changed
 */
public interface Operation {

    // should other tasks be informed about a state change
    // after this operation
    boolean isRequiredPublishSyncEvent();

    TaskSyncContext doOperation(TaskSyncContext taskSyncContext);
}
