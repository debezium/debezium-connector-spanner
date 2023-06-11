/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Dialect;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.spanner.config.SpannerTableFilter;
import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.connector.spanner.context.source.SourceInfoFactory;
import io.debezium.connector.spanner.context.source.SpannerSourceTaskContext;
import io.debezium.connector.spanner.db.DaoFactory;
import io.debezium.connector.spanner.db.DatabaseClientFactory;
import io.debezium.connector.spanner.db.SpannerChangeStreamFactory;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.kafka.KafkaAdminClientFactory;
import io.debezium.connector.spanner.kafka.KafkaPartitionInfoProvider;
import io.debezium.connector.spanner.metrics.SpannerChangeEventSourceMetricsFactory;
import io.debezium.connector.spanner.metrics.SpannerMeter;
import io.debezium.connector.spanner.processor.SourceRecordUtils;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.connector.spanner.processor.heartbeat.SpannerHeartbeatFactory;
import io.debezium.connector.spanner.processor.metadata.SpannerEventMetadataProvider;
import io.debezium.connector.spanner.schema.KafkaSpannerSchema;
import io.debezium.connector.spanner.schema.KafkaSpannerTableSchemaFactory;
import io.debezium.connector.spanner.task.LowWatermarkHolder;
import io.debezium.connector.spanner.task.PartitionOffsetProvider;
import io.debezium.connector.spanner.task.SynchronizationTaskContext;
import io.debezium.connector.spanner.task.SynchronizedPartitionManager;
import io.debezium.connector.spanner.task.TaskUid;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/** Spanner implementation for Debezium's CDC SourceTask */
public class SpannerConnectorTask extends SpannerBaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerConnectorTask.class);
    private static final String CONTEXT_NAME = "spanner-connector-task";
    private volatile ChangeEventQueue<DataChangeEvent> queue;

    private volatile SynchronizationTaskContext synchronizationTaskContext;

    private volatile String taskUid;

    private volatile SpannerMeter spannerMeter;

    private volatile LowWatermarkHolder lowWatermarkHolder;

    private volatile KafkaAdminClientFactory adminClientFactory;

    private volatile ChangeStream changeStream;

    private volatile SpannerEventDispatcher dispatcher;

    private volatile KafkaSpannerSchema schema;

    @Override
    protected SpannerChangeEventSourceCoordinator start(Configuration configuration) {

        final SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(configuration);

        this.taskUid = TaskUid.generateTaskUid(connectorConfig.getConnectorName(),
                connectorConfig.getTaskId());

        LOGGER.info("Starting task with uid: {}", taskUid);

        final DatabaseClientFactory databaseClientFactory = getDatabaseClientFactory(
                connectorConfig);

        final DaoFactory daoFactory = new DaoFactory(databaseClientFactory);

        final Dialect dialect = databaseClientFactory.getDatabaseClient().getDialect();

        final SpannerSourceTaskContext taskContext = new SpannerSourceTaskContext(connectorConfig,
                () -> spannerMeter.getCapturedTables());

        queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        final SpannerErrorHandler errorHandler = new SpannerErrorHandler(this, queue);

        this.spannerMeter = new SpannerMeter(
                this, connectorConfig, errorHandler, () -> lowWatermarkHolder.getLowWatermark());

        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        final DataCollectionFilters.DataCollectionFilter<TableId> tableFilter = new SpannerTableFilter(connectorConfig);

        final EventMetadataProvider metadataProvider = new SpannerEventMetadataProvider();

        final TopicNamingStrategy topicNamingStrategy = DefaultTopicNamingStrategy.create(connectorConfig);

        final SchemaRegistry schemaRegistry = new SchemaRegistry(
                connectorConfig.changeStreamName(),
                daoFactory.getSchemaDao(),
                () -> schema.resetCache());

        final KafkaSpannerTableSchemaFactory tableSchemaFactory = new KafkaSpannerTableSchemaFactory(
                topicNamingStrategy,
                schemaNameAdjuster,
                schemaRegistry,
                connectorConfig.getSourceInfoStructMaker().schema());

        schema = new KafkaSpannerSchema(tableSchemaFactory);

        final SpannerHeartbeatFactory spannerHeartbeatFactory = new SpannerHeartbeatFactory(connectorConfig, topicNamingStrategy, schemaNameAdjuster);

        final PartitionOffsetProvider partitionOffsetProvider = new PartitionOffsetProvider(
                this.context.offsetStorageReader(), spannerMeter.getMetricsEventPublisher());

        final SynchronizedPartitionManager partitionManager = new SynchronizedPartitionManager(
                event -> this.synchronizationTaskContext.publishEvent(event));

        final SpannerChangeStreamFactory spannerChangeStreamFactory = new SpannerChangeStreamFactory(
                this.taskUid,
                daoFactory,
                spannerMeter.getMetricsEventPublisher(),
                connectorConfig.getConnectorName(),
                dialect);

        this.changeStream = spannerChangeStreamFactory.getStream(
                connectorConfig.changeStreamName(),
                connectorConfig.getHeartbeatInterval(),
                connectorConfig.getMaxMissedHeartbeats());

        this.lowWatermarkHolder = new LowWatermarkHolder();

        final SourceInfoFactory sourceInfoFactory = new SourceInfoFactory(connectorConfig, lowWatermarkHolder);

        this.adminClientFactory = new KafkaAdminClientFactory(connectorConfig);

        final KafkaPartitionInfoProvider kafkaPartitionInfoProvider = new KafkaPartitionInfoProvider(adminClientFactory.getAdminClient());

        this.dispatcher = new SpannerEventDispatcher(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                tableFilter,
                new SpannerChangeEventCreator(),
                metadataProvider,
                spannerHeartbeatFactory,
                schemaNameAdjuster,
                schemaRegistry,
                sourceInfoFactory,
                kafkaPartitionInfoProvider);

        this.synchronizationTaskContext = new SynchronizationTaskContext(
                this,
                connectorConfig,
                errorHandler,
                partitionOffsetProvider,
                changeStream,
                dispatcher,
                adminClientFactory,
                schemaRegistry,
                this::finish,
                spannerMeter.getMetricsEventPublisher(),
                lowWatermarkHolder);

        final SpannerChangeEventSourceFactory changeEventSourceFactory = new SpannerChangeEventSourceFactory(
                connectorConfig,
                dispatcher,
                errorHandler,
                schemaRegistry,
                spannerMeter,
                changeStream,
                sourceInfoFactory,
                partitionManager);

        NotificationService<SpannerPartition, SpannerOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        this.coordinator = new SpannerChangeEventSourceCoordinator(
                getInitialOffsets(),
                errorHandler,
                SpannerConnector.class,
                connectorConfig,
                changeEventSourceFactory,
                new SpannerChangeEventSourceMetricsFactory(spannerMeter),
                dispatcher,
                schema,
                notificationService);

        this.spannerMeter.start();

        this.coordinator.start(taskContext, this.queue, metadataProvider);

        LOGGER.info("Before initialization task sync {}", taskUid);

        this.synchronizationTaskContext.init();

        LOGGER.info("Finished starting task {}", taskUid);

        return coordinator;
    }

    DatabaseClientFactory getDatabaseClientFactory(SpannerConnectorConfig connectorConfig) {
        return new DatabaseClientFactory(connectorConfig);
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        long pollAtTimestamp = Instant.now().toEpochMilli();

        List<SourceRecord> resultedRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .map(record -> SourceRecordUtils.addPollTimestamp(record, pollAtTimestamp))
                .collect(Collectors.toList());

        if (!resultedRecords.isEmpty()) {
            LOGGER.debug("Records sent to Kafka: {}", resultedRecords);
        }

        return resultedRecords;
    }

    @Override
    protected void onRecordSent(SourceRecord sourceRecord) {
        this.spannerMeter.getMetricsEventPublisher().logLatency(sourceRecord);
    }

    @Override
    protected void doStop() {
        LOGGER.info("Stopping task {}, changeStream", taskUid);

        changeStream.stop();

        LOGGER.info("Stopping task {}, synchronizationTaskContext", taskUid);

        synchronizationTaskContext.destroy();

        LOGGER.info("Stopping task {}, dispatcher", taskUid);

        dispatcher.destroy();

        LOGGER.info("Stopping task {}, adminClientFactory", taskUid);

        adminClientFactory.close();

        LOGGER.info("Stopping task {}, spannerMeter", taskUid);

        spannerMeter.shutdown();

        LOGGER.info("Task {} was stopped", taskUid);
    }

    public void finish() {
        this.queue.producerException(new ConnectException("Task " + this.taskUid + " finished work"));
    }

    public void restart() {
        this.queue.producerException(
                new RetriableException("Task " + this.taskUid + " will be restarted"));
    }

    public String getTaskUid() {
        return taskUid;
    }
}
