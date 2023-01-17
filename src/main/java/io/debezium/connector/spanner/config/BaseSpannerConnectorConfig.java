/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.spanner.config.validation.FieldValidator;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.schema.AbstractTopicNamingStrategy;

/** Provides all configuration properties for Spanner connector */
public abstract class BaseSpannerConnectorConfig extends CommonConnectorConfig {

    public static final String CONNECTOR_NAME_PROPERTY_NAME = "name";
    private static final String LOW_WATERMARK_ENABLED = "gcp.spanner.low-watermark.enabled";
    private static final String LOW_WATERMARK_UPDATE_PERIOD_MS = "gcp.spanner.low-watermark.update-period.ms";

    private static final String LOW_WATERMARK_STAMP_INTERVAL_NAME = "gcp.spanner.low-watermark.stamp.interval";

    private static final String TOPIC_DEFAULT_AUTO_CREATION_PARTITIONS_PROPERTY_NAME = "topic.creation.default.partitions";

    protected static final String GCP_SPANNER_PROJECT_ID_PROPERTY_NAME = "gcp.spanner.project.id";
    protected static final String GCP_SPANNER_INSTANCE_ID_PROPERTY_NAME = "gcp.spanner.instance.id";
    protected static final String GCP_SPANNER_DATABASE_ID_PROPERTY_NAME = "gcp.spanner.database.id";
    protected static final String GCP_SPANNER_HOST_PROPERTY_NAME = "gcp.spanner.host";
    protected static final String GCP_SPANNER_CHANGE_STREAM_PROPERTY_NAME = "gcp.spanner.change.stream";
    protected static final String START_TIME_PROPERTY_NAME = "gcp.spanner.start.time";
    protected static final String END_TIME_PROPERTY_NAME = "gcp.spanner.end.time";
    protected static final String GCP_SPANNER_CREDENTIALS_PATH_PROPERTY_NAME = "gcp.spanner.credentials.path";
    protected static final String GCP_SPANNER_CREDENTIALS_JSON_PROPERTY_NAME = "gcp.spanner.credentials.json";
    private static final String STREAM_EVENT_QUEUE_CAPACITY_PROPERTY_NAME = "gcp.spanner.stream.event.queue.capacity";

    private static final String TASK_STATE_CHANGE_EVENT_QUEUE_CAPACITY_PROPERTY_NAME = "connector.spanner.task.state.change.event.queue.capacity";

    private static final String VALUE_CAPTURE_MODE_PROPERTY_NAME = "gcp.spanner.value.capture.mode";

    private static final String TABLE_EXCLUDE_LIST_PROPERTY_NAME = "table.exclude.list";

    private static final String TABLE_INCLUDE_LIST_PROPERTY_NAME = "table.include.list";
    private static final String CONNECTOR_SPANNER_SYNC_TOPIC_PROPERTY_NAME = "connector.spanner.sync.topic";
    private static final String CONNECTOR_SPANNER_REBALANCING_TOPIC_PROPERTY_NAME = "connector.spanner.rebalancing.topic";
    private static final String CONNECTOR_SPANNER_REBALANCING_POLL_DURATION_PROPERTY_NAME = "connector.spanner.rebalancing.poll.duration";
    private static final String CONNECTOR_SPANNER_REBALANCING_COMMIT_OFFSETS_TIMEOUT_PROPERTY_NAME = "connector.spanner.rebalancing.commit.offsets.timeout";
    private static final String CONNECTOR_SPANNER_REBALANCING_COMMIT_OFFSET_INTERVAL_MS_PROPERTY_NAME = "connector.spanner.rebalancing.commit.offset.interval.ms";
    private static final String CONNECTOR_SPANNER_SYNC_POLL_DURATION_PROPERTY_NAME = "connector.spanner.sync.poll.duration";
    private static final String CONNECTOR_SPANNER_SYNC_REQUEST_TIMEOUT_PROPERTY_NAME = "connector.spanner.sync.request.timeout.ms";
    private static final String CONNECTOR_SPANNER_SYNC_DELIVERY_TIMEOUT_PROPERTY_NAME = "connector.spanner.sync.delivery.timeout.ms";
    private static final String CONNECTOR_SPANNER_SYNC_COMMIT_OFFSETS_TIMEOUT_PROPERTY_NAME = "connector.spanner.sync.commit.offsets.timeout";
    private static final String CONNECTOR_SPANNER_SYNC_COMMIT_OFFSET_INTERVAL_MS_PROPERTY_NAME = "connector.spanner.sync.commit.offset.interval.ms";

    private static final String CONNECTOR_SPANNER_SYNC_CLEANUP_POLICY_PROPERTY_NAME = "connector.spanner.sync.cleanup.policy";
    private static final String CONNECTOR_SPANNER_SYNC_RETENTION_MS_PROPERTY_NAME = "connector.spanner.sync.retention.ms";
    private static final String CONNECTOR_SPANNER_SYNC_SEGMENT_MS_POLICY_PROPERTY_NAME = "connector.spanner.sync.segment.ms";
    private static final String CONNECTOR_SPANNER_SYNC_MIN_CLEANABLE_DIRTY_RATIO_PROPERTY_NAME = "connector.spanner.sync.min.cleanable.dirty.ratio";
    private static final String CONNECTOR_SPANNER_SYNC_KAFKA_BOOTSTRAP_SERVERS_PROPERTY_NAME = "connector.spanner.sync.kafka.bootstrap.servers";

    private static final String CONNECTOR_SPANNER_PARTITION_FINISHING_AFTER_COMMIT_PROPERTY_NAME = "connector.spanner.partition.finishing.afterCommit";

    private static final String CONNECTOR_SPANNER_REBALANCING_TASK_WAITING_TIMEOUT_PROPERTY_NAME = "connector.spanner.rebalancing.task.wait.timeout";
    private static final String CONNECTOR_SPANNER_SYNC_EVENT_PUBLISH_WAITING_TIMEOUT_PROPERTY_NAME = "connector.spanner.sync.publisher.wait.timeout";

    private static final String MAX_MISSED_HEARTBEATS_PROPERTY_NAME = "connector.spanner.max.missed.heartbeats";

    private static final String MAX_TASKS_PROPERTY_NAME = "tasks.max";
    private static final String MIN_TASKS_PROPERTY_NAME = "tasks.min";
    private static final String DESIRED_PARTITIONS_TASKS_PROPERTY_NAME = "tasks.desired.partitions";

    private static final String SCALER_MONITOR_ENABLED_PROPERTY_NAME = "scaler.monitor.enabled";

    private static final String LOGGING_JSON_ENABLED_PROPERTY_NAME = "logging.json.enabled";

    private static final String DEFAULT_SYNC_TOPIC_PREFIX = "_sync_topic_spanner_connector_";
    private static final String DEFAULT_REBALANCING_TOPIC_PREFIX = "_rebalancing_topic_spanner_connector_";
    private static final String CONNECTOR_NAME_TEMPLATE = "<connector_name>";

    public static final String KAFKA_INTERNAL_CLIENT_CONFIG_PREFIX = "kafka.internal.client.";

    private static final String PERCENTAGE_METRICS_CLEAR_INTERVAL_PROPERTY_NAME = "connector.spanner.metrics.percentage.clear.interval";
    private static final String TASKS_FAIL_OVERLOADED_PROPERTY_NAME = "tasks.fail.overloaded";
    private static final String TASKS_FAIL_OVERLOADED_CHECK_INTERVAL_PROPERTY_NAME = "tasks.fail.overloaded.check.interval";

    protected static final Field LOW_WATERMARK_ENABLED_FIELD = Field.create(LOW_WATERMARK_ENABLED)
            .withDisplayName(LOW_WATERMARK_ENABLED)
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault(false)
            .withDescription("Defines if low-watermarks are enabled or not, default false");

    protected static final Field LOW_WATERMARK_UPDATE_PERIOD_MS_FIELD = Field.create(LOW_WATERMARK_UPDATE_PERIOD_MS)
            .withDisplayName(LOW_WATERMARK_UPDATE_PERIOD_MS)
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault(1000L)
            .withDescription("Low-watermark update period for each task, default 1000 ms.");

    protected static final Field TOPIC_DEFAULT_AUTO_CREATION_PARTITIONS_FIELD = Field.create(TOPIC_DEFAULT_AUTO_CREATION_PARTITIONS_PROPERTY_NAME)
            .withDisplayName("Topic auto creation num partitions")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 10))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault(1)
            .withDescription("Number of partitions in automatically created topic");

    public static final Field PROJECT_ID = Field.create(GCP_SPANNER_PROJECT_ID_PROPERTY_NAME)
            .withDisplayName("ProjectId")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withValidation(FieldValidator::isNotBlank)
            .withDescription("Spanner project id");

    public static final Field INSTANCE_ID = Field.create(GCP_SPANNER_INSTANCE_ID_PROPERTY_NAME)
            .withDisplayName("InstanceId")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 4))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withValidation(FieldValidator::isNotBlank)
            .withDescription("Spanner instance id");

    public static final Field DATABASE_ID = Field.create(GCP_SPANNER_DATABASE_ID_PROPERTY_NAME)
            .withDisplayName("DatabaseId")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 5))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withValidation(FieldValidator::isNotBlank)
            .withDescription("Spanner database id");

    public static final Field CHANGE_STREAM_NAME = Field.create(GCP_SPANNER_CHANGE_STREAM_PROPERTY_NAME)
            .withDisplayName("Change stream name")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 6))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(FieldValidator::isNotBlank)
            .withDescription("Spanner change stream name");

    public static final Field SPANNER_HOST = Field.create(GCP_SPANNER_HOST_PROPERTY_NAME)
            .withDisplayName("SpannerHost")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 7))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Spanner host");

    public static final Field SPANNER_CREDENTIALS_PATH = Field.create(GCP_SPANNER_CREDENTIALS_PATH_PROPERTY_NAME)
            .withDisplayName(GCP_SPANNER_CREDENTIALS_PATH_PROPERTY_NAME)
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 1))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withValidation(FieldValidator::isCorrectPath)
            .withDescription("Service account keys file path");

    public static final Field SPANNER_CREDENTIALS_JSON = Field.create(GCP_SPANNER_CREDENTIALS_JSON_PROPERTY_NAME)
            .withDisplayName(GCP_SPANNER_CREDENTIALS_JSON_PROPERTY_NAME)
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 2))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withValidation(FieldValidator::isCorrectJson)
            .withDescription("Service account keys with json format");

    public static final Field STREAM_EVENT_QUEUE_CAPACITY = Field.create(STREAM_EVENT_QUEUE_CAPACITY_PROPERTY_NAME)
            .withDisplayName("Change steam queue capacity")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 3))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(10000)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Change stream event queue capacity");

    public static final Field TASK_STATE_CHANGE_EVENT_QUEUE_CAPACITY = Field.create(TASK_STATE_CHANGE_EVENT_QUEUE_CAPACITY_PROPERTY_NAME)
            .withDisplayName("Task state change event queue capacity")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 4))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(1000)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Task state change event queue capacity");

    public static final Field START_TIME = Field.create(START_TIME_PROPERTY_NAME)
            .withDisplayName("Start time")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 7))
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withValidation(FieldValidator::isCorrectDateTime)
            .withDescription("Start change stream time");

    public static final Field END_TIME = Field.create(END_TIME_PROPERTY_NAME)
            .withDisplayName("End time")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 8))
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withValidation(FieldValidator::isCorrectDateTime)
            .withDescription("End change stream time");

    public static final Field SPANNER_HEART_BEAT_INTERVAL = Heartbeat.HEARTBEAT_INTERVAL
            .withValidation(FieldValidator::isCorrectHeartBeatInterval)
            .withDefault(300000);

    protected static final Field LOW_WATERMARK_STAMP_INTERVAL = Field.create(LOW_WATERMARK_STAMP_INTERVAL_NAME)
            .withDisplayName("Low watermark stamp interval (milli-seconds)")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 0))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "Length of an interval in milli-seconds in in which the connector periodically sends "
                            + "low watermark stamp messages to all table topics and all kafka partitions")
            .withDefault(10000)
            .withValidation(Field::isNonNegativeInteger);

    protected static final Field MAX_MISSED_HEARTBEATS = Field.create(MAX_MISSED_HEARTBEATS_PROPERTY_NAME)
            .withDisplayName("Maximum missed heartbeats to identify that partition gets stuck")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 0))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("Maximum missed heartbeats to identify that partition gets stuck")
            .withDefault(10)
            .withValidation(Field::isNonNegativeInteger);

    private static final Field VALUE_CAPTURE_MODE = Field.create(VALUE_CAPTURE_MODE_PROPERTY_NAME)
            .withDisplayName("Value capture mode")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 0))
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withValidation(FieldValidator::isCorrectCaptureMode)
            .withDefault("OLD_AND_NEW_VALUES")
            .withDescription("Value capture mode");

    protected static final Field TABLE_INCLUDE_LIST = Field.create(TABLE_INCLUDE_LIST_PROPERTY_NAME)
            .withDisplayName("Include Tables")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 0))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex)
            .withDescription("The tables for which changes are to be captured");

    protected static final Field TABLE_EXCLUDE_LIST = Field.create(TABLE_EXCLUDE_LIST_PROPERTY_NAME)
            .withDisplayName("Exclude Tables")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex)
            .withDescription(
                    "A comma-separated list of regular expressions that match the fully-qualified names"
                            + " of tables to be excluded from monitoring");

    protected static final Field SYNC_TOPIC = Field.create(CONNECTOR_SPANNER_SYNC_TOPIC_PROPERTY_NAME)
            .withDisplayName("Sync Topic Prefix")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 20))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_SYNC_TOPIC_PREFIX)
            .withDescription(
                    "Connector Sync topic name, default "
                            + DEFAULT_SYNC_TOPIC_PREFIX
                            + CONNECTOR_NAME_TEMPLATE);
    protected static final Field REBALANCING_TOPIC = Field.create(CONNECTOR_SPANNER_REBALANCING_TOPIC_PROPERTY_NAME)
            .withDisplayName("Rebalancing Topic Prefix")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 21))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_REBALANCING_TOPIC_PREFIX)
            .withDescription(
                    "Connector Rebalancing topic name, default "
                            + DEFAULT_REBALANCING_TOPIC_PREFIX
                            + CONNECTOR_NAME_TEMPLATE);

    protected static final Field REBALANCING_POLL_DURATION = Field.create(CONNECTOR_SPANNER_REBALANCING_POLL_DURATION_PROPERTY_NAME)
            .withDisplayName("Rebalancing Topic Poll Duration")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 25))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(5000)
            .withDescription("Connector rebalancing topic poll duration" + ", default 5000 ms");

    protected static final Field REBALANCING_COMMIT_OFFSETS_TIMEOUT = Field.create(CONNECTOR_SPANNER_REBALANCING_COMMIT_OFFSETS_TIMEOUT_PROPERTY_NAME)
            .withDisplayName("Rebalancing Topic Commit Offsets Timeout")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 26))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(5000)
            .withDescription(
                    "Connector rebalancing topic commit offsets timeout" + ", default 5000 ms");

    protected static final Field SYNC_POLL_DURATION = Field.create(CONNECTOR_SPANNER_SYNC_POLL_DURATION_PROPERTY_NAME)
            .withDisplayName("Sync Topic Poll Duration")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 27))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(500)
            .withDescription("Connector sync topic poll duration" + ", default 500 ms");

    protected static final Field SYNC_COMMIT_OFFSETS_TIMEOUT = Field.create(CONNECTOR_SPANNER_SYNC_COMMIT_OFFSETS_TIMEOUT_PROPERTY_NAME)
            .withDisplayName("Sync Topic Commit Offsets Timeout")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 28))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(5000)
            .withDescription("Connector sync topic commit offsets timeout" + ", default 5000 ms");

    protected static final Field SYNC_KAFKA_BOOTSTRAP_SERVERS = Field.create(CONNECTOR_SPANNER_SYNC_KAFKA_BOOTSTRAP_SERVERS_PROPERTY_NAME)
            .withDisplayName("Sync Kafka Bootstrap Servers")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 30))
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("Kafka bootstrapServers for synchronization between tasks");

    protected static final Field SYNC_REQUEST_TIMEOUT = Field.create(CONNECTOR_SPANNER_SYNC_REQUEST_TIMEOUT_PROPERTY_NAME)
            .withDisplayName("Sync Request Timeout")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 31))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(5000)
            .withDescription(
                    "Connector Sync topic property: request.timeout.ms" + ", default 5000 ms");

    protected static final Field SYNC_DELIVERY_TIMEOUT = Field.create(CONNECTOR_SPANNER_SYNC_DELIVERY_TIMEOUT_PROPERTY_NAME)
            .withDisplayName("Sync Delivery Timeout")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 32))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(15000)
            .withDescription(
                    "Connector Sync topic property: delivery.timeout.ms" + ", default 15000 ms");

    protected static final Field SYNC_CLEANUP_POLICY = Field.create(CONNECTOR_SPANNER_SYNC_CLEANUP_POLICY_PROPERTY_NAME)
            .withDisplayName("Sync Topic property: cleanup.policy")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 33))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault("delete")
            .withDescription("Sync Topic property: cleanup.policy, default: delete");

    protected static final Field SYNC_RETENTION_MS = Field.create(CONNECTOR_SPANNER_SYNC_RETENTION_MS_PROPERTY_NAME)
            .withDisplayName("Sync Topic property: retention.ms")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 34))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(86400000)
            .withDescription("Sync Topic property: retention.ms, default: 86400000 (24h)");

    protected static final Field SYNC_SEGMENT_MS = Field.create(CONNECTOR_SPANNER_SYNC_SEGMENT_MS_POLICY_PROPERTY_NAME)
            .withDisplayName("Sync Topic property: segment.ms")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 35))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(43200000)
            .withDescription("Sync Topic property: segment.ms, default: 43200000 (12h)");

    protected static final Field SYNC_MIN_CLEANABLE_DIRTY_RATIO = Field.create(CONNECTOR_SPANNER_SYNC_MIN_CLEANABLE_DIRTY_RATIO_PROPERTY_NAME)
            .withDisplayName("Sync Topic property: min.cleanable.dirty.ratio")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 36))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault("0.1")
            .withDescription("Sync Topic property: min.cleanable.dirty.ratio, default: 0.1");

    protected static final Field SYNC_COMMIT_OFFSETS_INTERVAL_MS = Field.create(CONNECTOR_SPANNER_SYNC_COMMIT_OFFSET_INTERVAL_MS_PROPERTY_NAME)
            .withDisplayName("Sync Topic Commit Offsets Interval")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 37))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(60_000)
            .withDescription("Connector sync topic commit offsets interval" + ", default 60000 ms");

    protected static final Field REBALANCING_COMMIT_OFFSETS_INTERVAL_MS = Field.create(CONNECTOR_SPANNER_REBALANCING_COMMIT_OFFSET_INTERVAL_MS_PROPERTY_NAME)
            .withDisplayName("Rebalancing Topic Commit Offsets Interval")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 38))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(60_000)
            .withDescription(
                    "Connector rebalancing topic commit offsets interval" + ", default 60000 ms");

    protected static final Field TASKS_FAIL_OVERLOADED = Field.create(TASKS_FAIL_OVERLOADED_PROPERTY_NAME)
            .withDisplayName("Fail Overloaded Task")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 39))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("Task will be failed, if it is overloaded by partitions");

    protected static final Field TASKS_FAIL_OVERLOADED_CHECK_INTERVAL = Field.create(TASKS_FAIL_OVERLOADED_CHECK_INTERVAL_PROPERTY_NAME)
            .withDisplayName("Check Interval for \"Fail Overloaded Task\"")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 40))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(5000)
            .withDescription(
                    "Interval in milliseconds to check whether the task is overloaded by partitions");

    protected static final Field MAX_TASKS = Field.create(MAX_TASKS_PROPERTY_NAME)
            .withDisplayName("Max Tasks")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 10))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault(10)
            .withDescription("Maximum number of tasks in connector");

    protected static final Field MIN_TASKS = Field.create(MIN_TASKS_PROPERTY_NAME)
            .withDisplayName("Min Tasks")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 11))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault(2)
            .withDescription("Minimum number of tasks in connector");

    protected static final Field DESIRED_PARTITIONS_TASKS = Field.create(DESIRED_PARTITIONS_TASKS_PROPERTY_NAME)
            .withDisplayName("Desired partitions per task")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 12))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault(2)
            .withDescription(
                    "Connector will increase task number, "
                            + "if actual partitions per task ratio become > desired up to "
                            + MAX_TASKS_PROPERTY_NAME
                            + " bound");

    protected static final Field SCALER_MONITOR_ENABLED = Field.create(SCALER_MONITOR_ENABLED_PROPERTY_NAME)
            .withDisplayName("Scaler monitor enabled")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 13))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(false)
            .withDescription("Defines if Scaler Monitor enabled or not, default false");

    protected static final Field LOGGING_JSON_ENABLED = Field.create(LOGGING_JSON_ENABLED_PROPERTY_NAME)
            .withDisplayName("Logging json enabled")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 13))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(false)
            .withDescription("Defines if json logging enabled or not, default false");

    protected static final Field CONNECTOR_SPANNER_PARTITION_FINISHING_AFTER_COMMIT_FIELD = Field.create(CONNECTOR_SPANNER_PARTITION_FINISHING_AFTER_COMMIT_PROPERTY_NAME)
            .withDisplayName("Connector spanner partition finishing after commit strategy")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 14))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(true)
            .withDescription("default true");

    protected static final Field REBALANCING_TASK_WAITING_TIMEOUT = Field.create(CONNECTOR_SPANNER_REBALANCING_TASK_WAITING_TIMEOUT_PROPERTY_NAME)
            .withDisplayName("Rebalancing Task waiting timeout")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 16))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(1000)
            .withDescription("Connector rebalancing task waiting timeout, default 1000 ms");

    protected static final Field SYNC_EVENT_PUBLISH_WAITING_TIMEOUT = Field.create(CONNECTOR_SPANNER_SYNC_EVENT_PUBLISH_WAITING_TIMEOUT_PROPERTY_NAME)
            .withDisplayName("Sync Event Publisher waiting timeout")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 17))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(5)
            .withDescription("Connector rebalancing task waiting timeout, default 5 ms");

    protected static final Field PERCENTAGE_METRICS_CLEAR_INTERVAL = Field.create(PERCENTAGE_METRICS_CLEAR_INTERVAL_PROPERTY_NAME)
            .withDisplayName("Percentage metrics clear interval")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 18))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(10000)
            .withDescription("Percentage metrics clear interval");

    protected static final ConfigDefinition CONFIG_DEFINITION = ConfigDefinition.editor()
            .name("Spanner")
            .type(PROJECT_ID)
            .connector(
                    INSTANCE_ID,
                    DATABASE_ID,
                    CHANGE_STREAM_NAME,
                    START_TIME,
                    END_TIME,
                    SPANNER_CREDENTIALS_PATH,
                    SPANNER_CREDENTIALS_JSON,
                    SPANNER_HOST,
                    STREAM_EVENT_QUEUE_CAPACITY,
                    TASK_STATE_CHANGE_EVENT_QUEUE_CAPACITY,
                    VALUE_CAPTURE_MODE,
                    SPANNER_HEART_BEAT_INTERVAL,
                    MAX_BATCH_SIZE,
                    MAX_QUEUE_SIZE,
                    POLL_INTERVAL_MS,
                    MAX_QUEUE_SIZE_IN_BYTES,
                    SKIPPED_OPERATIONS,
                    QUERY_FETCH_SIZE,
                    REBALANCING_TOPIC,
                    REBALANCING_POLL_DURATION,
                    REBALANCING_COMMIT_OFFSETS_TIMEOUT,
                    REBALANCING_COMMIT_OFFSETS_INTERVAL_MS,
                    REBALANCING_TASK_WAITING_TIMEOUT,
                    SYNC_EVENT_PUBLISH_WAITING_TIMEOUT,
                    CONNECTOR_SPANNER_PARTITION_FINISHING_AFTER_COMMIT_FIELD,
                    PERCENTAGE_METRICS_CLEAR_INTERVAL,
                    SYNC_TOPIC,
                    TOPIC_DEFAULT_AUTO_CREATION_PARTITIONS_FIELD,
                    SYNC_KAFKA_BOOTSTRAP_SERVERS,
                    SYNC_POLL_DURATION,
                    SYNC_COMMIT_OFFSETS_TIMEOUT,
                    SYNC_COMMIT_OFFSETS_INTERVAL_MS,
                    SYNC_REQUEST_TIMEOUT,
                    SYNC_DELIVERY_TIMEOUT,
                    SYNC_CLEANUP_POLICY,
                    SYNC_RETENTION_MS,
                    SYNC_SEGMENT_MS,
                    SYNC_MIN_CLEANABLE_DIRTY_RATIO,
                    MAX_TASKS,
                    MIN_TASKS,
                    DESIRED_PARTITIONS_TASKS,
                    TASKS_FAIL_OVERLOADED,
                    TASKS_FAIL_OVERLOADED_CHECK_INTERVAL,
                    SCALER_MONITOR_ENABLED,
                    LOGGING_JSON_ENABLED)
            .events(
                    TABLE_EXCLUDE_LIST,
                    TABLE_INCLUDE_LIST,
                    CUSTOM_CONVERTERS,
                    SANITIZE_FIELD_NAMES,
                    TOMBSTONES_ON_DELETE,
                    AbstractTopicNamingStrategy.TOPIC_HEARTBEAT_PREFIX)
            .create();
    private static final int POLL_INTERVAL_IN_MS = 25;

    protected final String startTime;

    protected BaseSpannerConnectorConfig(
                                         Configuration config, String logicalName, int defaultSnapshotFetchSize) {
        super(
                Configuration.from(config.asProperties())
                        .edit()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, logicalName)
                        .with(CommonConnectorConfig.POLL_INTERVAL_MS, POLL_INTERVAL_IN_MS)
                        .build(),
                defaultSnapshotFetchSize);
        this.startTime = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
    }

    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }
}
