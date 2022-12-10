/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
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
        Set<String> tokens = getAllActivePartitions(taskSyncContext);
        if (Timestamp.now().toSqlTimestamp().after(endTime.toSqlTimestamp())) {
            if (tokens.isEmpty()) {
                finishingWorkHandler.run();
                LOGGER.info("Connector finished work, end time reached");
                isRequiredPublishSyncEvent = true;
                return taskSyncContext.toBuilder().finished(true).build();
            }
            LOGGER.warn("End time reached, but partitions are processing: {}", tokens);

        }
        return taskSyncContext;
    }

    private Set<String> getAllActivePartitions(TaskSyncContext taskSyncContext) {
        return taskSyncContext.getAllTaskStates().values().stream()
                .flatMap(taskState -> Stream.concat(taskState.getPartitions().stream(), taskState.getSharedPartitions().stream()))
                .filter(partitionState -> !partitionState.getState().equals(PartitionStateEnum.FINISHED)
                        && !partitionState.getState().equals(PartitionStateEnum.REMOVED))
                .map(PartitionState::getToken)
                .collect(Collectors.toSet());
    }
}
