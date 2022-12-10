/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.spanner.config.validation.ConfigurationValidator;
import io.debezium.connector.spanner.kafka.KafkaAdminClientFactory;
import io.debezium.connector.spanner.kafka.internal.KafkaRebalanceTopicAdminService;
import io.debezium.connector.spanner.task.LoggerUtils;
import io.debezium.connector.spanner.task.scaler.TaskScalerMonitor;
import io.debezium.connector.spanner.task.scaler.TaskScalerMonitorFactory;

/**
 * Provides implementation for the Spanner Source Connector
 */
public class SpannerConnector extends SourceConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerConnector.class);

    private Map<String, String> props = Map.of();
    private volatile TaskScalerMonitor taskScalerMonitor;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SpannerConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;

        LOGGER.info("Starting connector: props {}", props);

        final SpannerConnectorConfig config = new SpannerConnectorConfig(Configuration.from(props));

        createRebalanceTopic(config);

        TaskScalerMonitorFactory scalerFactory = new TaskScalerMonitorFactory(config, context, this::onError);

        taskScalerMonitor = scalerFactory.createMonitor();

        try {
            taskScalerMonitor.start();
        }
        catch (Exception ex) {
            taskScalerMonitor.shutdown();
            throw new RuntimeException(ex);
        }

        if (config.isLoggingJsonEnabled()) {
            LoggerUtils.enableJsonLog();
        }
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        int tasksCount = taskScalerMonitor.getRequiredTasksCount();

        LOGGER.info("taskConfigs: tasksCount: {}", tasksCount);

        return IntStream.range(0, tasksCount).mapToObj(this::getProps).collect(Collectors.toList());
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        return ConfigurationValidator.validate(connectorConfigs);
    }

    @Override
    public void stop() {
        LOGGER.info("stopping connector");
        this.props = null;

        this.taskScalerMonitor.shutdown();
    }

    @Override
    public ConfigDef config() {
        return SpannerConnectorConfig.configDef();
    }

    private void onError(RuntimeException exception) {
        taskScalerMonitor.shutdown();
        throw exception;
    }

    @VisibleForTesting
    Map<String, String> getProps(int taskId) {
        Map<String, String> taskProps = new HashMap<>(props);
        taskProps.put(CommonConnectorConfig.TASK_ID, String.valueOf(taskId));
        return taskProps;
    }

    @VisibleForTesting
    void createRebalanceTopic(SpannerConnectorConfig config) {
        KafkaAdminClientFactory adminClientFactory = new KafkaAdminClientFactory(config);
        final KafkaRebalanceTopicAdminService rebalanceTopicAdminService = new KafkaRebalanceTopicAdminService(adminClientFactory.getAdminClient(), config);
        rebalanceTopicAdminService.createAdjustRebalanceTopic();
        adminClientFactory.close();
    }

}
