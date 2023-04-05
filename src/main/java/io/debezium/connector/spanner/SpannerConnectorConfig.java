/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.config.Configuration;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.spanner.config.BaseSpannerConnectorConfig;
import io.debezium.connector.spanner.context.source.SourceInfo;

/**
 * Configuration API for the Spanner connector
 */
public class SpannerConnectorConfig extends BaseSpannerConnectorConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerConnectorConfig.class);

    public SpannerConnectorConfig(Configuration config) {
        super(config, config.getString(BaseSpannerConnectorConfig.CONNECTOR_NAME_PROPERTY_NAME), 0);
        LOGGER.debug("prop fields {}", SpannerConnectorConfig.ALL_FIELDS);
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return getConfig().getString(CONNECTOR_NAME_PROPERTY_NAME);
    }

    @Override
    protected SourceInfoStructMaker<SourceInfo> getSourceInfoStructMaker(Version version) {
        return new SpannerSourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    @Override
    public boolean isSchemaChangesHistoryEnabled() {
        return false;
    }

    @Override
    public boolean isSchemaCommentsHistoryEnabled() {
        return false;
    }

    @Override
    public Duration getHeartbeatInterval() {
        return getConfig().getDuration(SPANNER_HEART_BEAT_INTERVAL, ChronoUnit.MILLIS);
    }

    @Override
    public boolean shouldProvideTransactionMetadata() {
        return false;
    }

    @Override
    public EventProcessingFailureHandlingMode getEventProcessingFailureHandlingMode() {
        return EventProcessingFailureHandlingMode.FAIL;
    }

    public Properties kafkaProps(Map<?, ?> props) {
        Properties properties = new Properties();
        Map<String, String> spannerKafkaProps = this.getConfig()
                .subset(KAFKA_INTERNAL_CLIENT_CONFIG_PREFIX, true).asMap();
        if (!spannerKafkaProps.isEmpty()) {
            properties.putAll(spannerKafkaProps);
        }
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.bootStrapServer());
        properties.put("max.request.size", 104858800);
        properties.put("max.partition.fetch.bytes", 104858800);

        properties.putAll(props);

        return properties;
    }

    public int getMaxMissedHeartbeats() {
        return getConfig().getInteger(MAX_MISSED_HEARTBEATS);
    }

    public Duration getLowWatermarkStampInterval() {
        return getConfig().getDuration(LOW_WATERMARK_STAMP_INTERVAL, ChronoUnit.MILLIS);
    }

    public String projectId() {
        return getConfig().getString(GCP_SPANNER_PROJECT_ID_PROPERTY_NAME);
    }

    public String instanceId() {
        return getConfig().getString(GCP_SPANNER_INSTANCE_ID_PROPERTY_NAME);
    }

    public String databaseId() {
        return getConfig().getString(GCP_SPANNER_DATABASE_ID_PROPERTY_NAME);
    }

    public String databaseRole() {
        return getConfig().getString(GCP_SPANNER_DATABASE_ROLE_PROPERTY_NAME);
    }

    public String spannerHost() {
        return getConfig().getString(GCP_SPANNER_HOST_PROPERTY_NAME);
    }

    public String changeStreamName() {
        return getConfig().getString(GCP_SPANNER_CHANGE_STREAM_PROPERTY_NAME);
    }

    public Timestamp startTime() {
        String timestamp = getConfig().getString(START_TIME_PROPERTY_NAME, startTime);
        return Timestamp.parseTimestamp(timestamp);
    }

    public Timestamp endTime() {
        String timestamp = getConfig().getString(END_TIME_PROPERTY_NAME);
        return timestamp == null ? null : Timestamp.parseTimestamp(timestamp);
    }

    public int queueCapacity() {
        return getConfig().getInteger(STREAM_EVENT_QUEUE_CAPACITY, (int) STREAM_EVENT_QUEUE_CAPACITY.defaultValue());
    }

    public String gcpSpannerCredentialsJson() {
        return getConfig().getString(GCP_SPANNER_CREDENTIALS_JSON_PROPERTY_NAME);
    }

    public String gcpSpannerCredentialsPath() {
        return getConfig().getString(GCP_SPANNER_CREDENTIALS_PATH_PROPERTY_NAME);
    }

    public String tableExcludeList() {
        return getConfig().getString(TABLE_EXCLUDE_LIST);
    }

    public String tableIncludeList() {
        return getConfig().getString(TABLE_INCLUDE_LIST);
    }

    public String bootStrapServer() {

        // try to get configured value, if it is specified
        String valueFromConfig = getConfig().getString(SYNC_KAFKA_BOOTSTRAP_SERVERS);
        if (StringUtils.isNotBlank(valueFromConfig)) {
            return valueFromConfig;
        }

        // other way - try to get the same as for connector
        String valueFromConnect = System.getenv("CONNECT_BOOTSTRAP_SERVERS");
        if (StringUtils.isNotBlank(valueFromConnect)) {
            return valueFromConnect;
        }

        // Return default for the local dev
        return "localhost:9092";
    }

    public String rebalancingTopic() {
        return getConfig().getString(REBALANCING_TOPIC) + getConnectorName();
    }

    public int rebalancingPollDuration() {
        return getConfig().getInteger(REBALANCING_POLL_DURATION);
    }

    public int rebalancingCommitOffsetsTimeout() {
        return getConfig().getInteger(REBALANCING_COMMIT_OFFSETS_TIMEOUT);
    }

    public int rebalancingCommitOffsetsInterval() {
        return getConfig().getInteger(REBALANCING_COMMIT_OFFSETS_INTERVAL_MS);
    }

    public Duration rebalancingTaskWaitingTimeout() {
        return getConfig().getDuration(REBALANCING_TASK_WAITING_TIMEOUT, ChronoUnit.MILLIS);
    }

    public int syncEventPublisherWaitingTimeout() {
        return getConfig().getInteger(SYNC_EVENT_PUBLISH_WAITING_TIMEOUT);
    }

    public int syncPollDuration() {
        return getConfig().getInteger(SYNC_POLL_DURATION);
    }

    public int syncCommitOffsetsTimeout() {
        return getConfig().getInteger(SYNC_COMMIT_OFFSETS_TIMEOUT);
    }

    public int syncCommitOffsetsInterval() {
        return getConfig().getInteger(SYNC_COMMIT_OFFSETS_INTERVAL_MS);
    }

    public int syncRequestTimeout() {
        return getConfig().getInteger(SYNC_REQUEST_TIMEOUT);
    }

    public int syncDeliveryTimeout() {
        return getConfig().getInteger(SYNC_DELIVERY_TIMEOUT);
    }

    public String taskSyncTopic() {
        return getConfig().getString(SYNC_TOPIC) + getConnectorName();
    }

    public String syncCleanupPolicy() {
        return getConfig().getString(SYNC_CLEANUP_POLICY);
    }

    public int syncRetentionMs() {
        return getConfig().getInteger(SYNC_RETENTION_MS);
    }

    public int syncSegmentMs() {
        return getConfig().getInteger(SYNC_SEGMENT_MS);
    }

    public String syncMinCleanableDirtyRatio() {
        return getConfig().getString(SYNC_MIN_CLEANABLE_DIRTY_RATIO);
    }

    public int getMaxTasks() {
        return getConfig().getInteger(MAX_TASKS);
    }

    public int getMinTasks() {
        return getConfig().getInteger(MIN_TASKS);
    }

    public int getDesiredPartitionsTasks() {
        return getConfig().getInteger(DESIRED_PARTITIONS_TASKS);
    }

    public boolean isLowWatermarkEnabled() {
        return getConfig().getBoolean(LOW_WATERMARK_ENABLED_FIELD);
    }

    public long getLowWatermarkUpdatePeriodMs() {
        return getConfig().getLong(LOW_WATERMARK_UPDATE_PERIOD_MS_FIELD,
                (Long) LOW_WATERMARK_UPDATE_PERIOD_MS_FIELD.defaultValue());
    }

    public boolean isScalerMonitorEnabled() {
        return getConfig().getBoolean(SCALER_MONITOR_ENABLED);
    }

    public boolean isLoggingJsonEnabled() {
        return getConfig().getBoolean(LOGGING_JSON_ENABLED);
    }

    public boolean isFinishingPartitionAfterCommit() {
        return getConfig().getBoolean(CONNECTOR_SPANNER_PARTITION_FINISHING_AFTER_COMMIT_FIELD);
    }

    public int taskStateChangeEventQueueCapacity() {
        return getConfig().getInteger(TASK_STATE_CHANGE_EVENT_QUEUE_CAPACITY,
                (int) TASK_STATE_CHANGE_EVENT_QUEUE_CAPACITY.defaultValue());
    }

    public Duration percentageMetricsClearInterval() {
        return getConfig().getDuration(PERCENTAGE_METRICS_CLEAR_INTERVAL, ChronoUnit.MILLIS);
    }

    public boolean failOverloadedTask() {
        return getConfig().getBoolean(TASKS_FAIL_OVERLOADED);
    }

    public long failOverloadedTaskInterval() {
        return getConfig().getLong(TASKS_FAIL_OVERLOADED_CHECK_INTERVAL);
    }

    public int getTopicNumPartitions() {
        return getConfig().getInteger(TOPIC_DEFAULT_AUTO_CREATION_PARTITIONS_FIELD);
    }

    public String syncTopicMaxMessageSize() {
        return getConfig().getString(SYNC_TOPIC_MAX_MESSAGE_BYTES);
    }
}
