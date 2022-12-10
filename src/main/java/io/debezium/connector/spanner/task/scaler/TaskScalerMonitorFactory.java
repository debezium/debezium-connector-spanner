/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.scaler;

import java.util.UUID;
import java.util.function.Consumer;

import org.apache.kafka.connect.connector.ConnectorContext;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.kafka.internal.SyncEventConsumerFactory;
import io.debezium.connector.spanner.kafka.internal.TaskSyncEventListener;

/**
 * Creates {@link TaskScalerMonitor} based on configuration
 */
public class TaskScalerMonitorFactory {
    private final SpannerConnectorConfig connectorConfig;
    private final ConnectorContext connectorContext;
    private final Consumer<RuntimeException> errorHandler;

    public TaskScalerMonitorFactory(SpannerConnectorConfig connectorConfig, ConnectorContext connectorContext, Consumer<RuntimeException> errorHandler) {
        this.connectorConfig = connectorConfig;
        this.connectorContext = connectorContext;
        this.errorHandler = errorHandler;
    }

    public TaskScalerMonitor createMonitor() {
        if (connectorConfig.isScalerMonitorEnabled()) {
            return createScalingTaskMonitor();
        }
        return createFixedTaskMonitor();
    }

    private TaskScalerMonitor createScalingTaskMonitor() {
        String consumerGroup = "scaler-group-" + UUID.randomUUID();
        SyncEventConsumerFactory<String, byte[]> syncEventConsumerFactory = new SyncEventConsumerFactory<>(connectorConfig, true);

        TaskSyncEventListener syncEventListener = new TaskSyncEventListener(
                consumerGroup,
                connectorConfig.taskSyncTopic(),
                syncEventConsumerFactory,
                false,
                errorHandler);

        return new TaskScalerMonitorImpl(syncEventListener, new TaskScaler(connectorConfig, connectorContext), connectorConfig.getMinTasks());
    }

    private TaskScalerMonitor createFixedTaskMonitor() {
        return new FixedTaskScalerMonitorImpl(connectorConfig.getMaxTasks());
    }
}
