/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.task.TaskStateUtil;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Notify user about the finishing of connector work
 */
public class ConnectorEndDetectionOperation implements Operation {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorEndDetectionOperation.class);

    private final Runnable finishingWorkHandler;

    private final Timestamp endTime;

    private boolean isRequiredPublishSyncEvent = false;

    public ConnectorEndDetectionOperation(Runnable finishingWorkHandler, Timestamp endTime) {
        this.finishingWorkHandler = finishingWorkHandler;
        this.endTime = endTime;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return isRequiredPublishSyncEvent;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        if (endTime == null || taskSyncContext.isFinished()) {
            return taskSyncContext;
        }
        if (Timestamp.now().toSqlTimestamp().after(endTime.toSqlTimestamp())) {
            if (TaskStateUtil.totalInProgressPartitions(taskSyncContext) == 0
                    && TaskStateUtil.totalFinishedPartitions(taskSyncContext) > 0) {
                finishingWorkHandler.run();
                LOGGER.info("Connector finished work, end time reached");
                isRequiredPublishSyncEvent = true;
                return taskSyncContext.toBuilder().finished(true).build();
            }
        }
        return taskSyncContext;
    }

    @Override
    public List<String> updatedOwnedPartitions() {
        return Collections.emptyList();
    }

    @Override
    public List<String> updatedSharedPartitions() {
        return Collections.emptyList();
    }

    @Override
    public List<String> removedOwnedPartitions() {
        return Collections.emptyList();
    }

    @Override
    public List<String> removedSharedPartitions() {
        return Collections.emptyList();
    }
}
