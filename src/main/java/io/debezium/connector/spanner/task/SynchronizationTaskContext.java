/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerConnectorTask;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.kafka.KafkaAdminClientFactory;
import io.debezium.connector.spanner.kafka.internal.KafkaConsumerAdminService;
import io.debezium.connector.spanner.kafka.internal.ProducerFactory;
import io.debezium.connector.spanner.kafka.internal.RebalancingConsumerFactory;
import io.debezium.connector.spanner.kafka.internal.RebalancingEventListener;
import io.debezium.connector.spanner.kafka.internal.SyncEventConsumerFactory;
import io.debezium.connector.spanner.kafka.internal.TaskSyncEventListener;
import io.debezium.connector.spanner.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.connector.spanner.task.leader.LeaderAction;
import io.debezium.connector.spanner.task.leader.LeaderService;
import io.debezium.connector.spanner.task.leader.LowWatermarkStampPublisher;
import io.debezium.connector.spanner.task.leader.rebalancer.LeaderRebalanceStrategy;
import io.debezium.connector.spanner.task.leader.rebalancer.TaskPartitionEqualSharingRebalancer;
import io.debezium.connector.spanner.task.leader.rebalancer.TaskPartitionGreedyLeaderRebalancer;
import io.debezium.connector.spanner.task.leader.rebalancer.TaskPartitionRebalancer;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;
import io.debezium.pipeline.ErrorHandler;

/**
 * This class coordinates between the connector producers and consumers:
 * The RebalancingEventListener producer produces events that are consumed by the RebalanceHandler.
 * The TaskSyncEventListener produces events that are consumed by the SyncEventHandler.
 * The SynchronizedPartitionManager produces events to the queue, which are then consumed from
 * by the TaskStateChangeEventHandler.
 */
public class SynchronizationTaskContext {
    private static final Logger LOGGER = getLogger(SynchronizationTaskContext.class);

    private final LeaderRebalanceStrategy leaderRebalanceStrategy = LeaderRebalanceStrategy.EQUAL_SHARING;
    private final SyncEventConsumerFactory<String, byte[]> syncEventConsumerFactory;
    private final RebalancingConsumerFactory<?, ?> rebalancingConsumerFactory;
    private final ProducerFactory<String, byte[]> producerFactory;

    private final LeaderAction leaderAction;

    private final RebalancingEventListener rebalancingEventListener;
    private final TaskSyncEventListener taskSyncEventListener;
    private final TaskSyncPublisher taskSyncPublisher;

    private final TaskSyncContextHolder taskSyncContextHolder;

    private final TaskStateChangeEventHandler taskStateChangeEventHandler;

    private final ErrorHandler errorHandler;

    private final PartitionFactory partitionFactory;

    private final LowWatermarkStampPublisher lowWatermarkStampPublisher;

    private final Runnable finishingHandler;

    private final TaskStateChangeEventProcessor taskStateChangeEventProcessor;

    private final SyncEventHandler syncEventHandler;

    private final RebalanceHandler rebalanceHandler;

    private final LowWatermarkCalculationJob lowWatermarkCalculationJob;

    private final SchemaRegistry schemaRegistry;

    private final SpannerConnectorTask task;

    private final SpannerConnectorConfig connectorConfig;

    public SynchronizationTaskContext(SpannerConnectorTask task,
                                      SpannerConnectorConfig connectorConfig,
                                      ErrorHandler errorHandler,
                                      PartitionOffsetProvider partitionOffsetProvider,
                                      ChangeStream changeStream,
                                      SpannerEventDispatcher spannerEventDispatcher,
                                      KafkaAdminClientFactory adminClientFactory,
                                      SchemaRegistry schemaRegistry,
                                      Runnable finishingHandler,
                                      MetricsEventPublisher metricsEventPublisher,
                                      LowWatermarkHolder lowWatermarkHolder) {
        final String rebalancingTopic = connectorConfig.rebalancingTopic();
        final String taskSyncTopic = connectorConfig.taskSyncTopic();
        final String connectorName = connectorConfig.getConnectorName();

        this.task = task;

        this.connectorConfig = connectorConfig;

        this.errorHandler = errorHandler;

        this.finishingHandler = finishingHandler;

        this.schemaRegistry = schemaRegistry;

        this.syncEventConsumerFactory = new SyncEventConsumerFactory<>(connectorConfig, false);
        this.rebalancingConsumerFactory = new RebalancingConsumerFactory<>(connectorConfig);
        this.producerFactory = new ProducerFactory(connectorConfig);

        this.taskSyncContextHolder = new TaskSyncContextHolder(metricsEventPublisher);

        this.taskSyncPublisher = new TaskSyncPublisher(task.getTaskUid(), taskSyncTopic, connectorConfig.syncEventPublisherWaitingTimeout(), producerFactory,
                this::onError);

        final KafkaConsumerAdminService kafkaAdminService = new KafkaConsumerAdminService(adminClientFactory.getAdminClient(), connectorName);

        this.partitionFactory = new PartitionFactory(partitionOffsetProvider, metricsEventPublisher);

        final LeaderService leaderService = new LeaderService(taskSyncContextHolder,
                connectorConfig,
                this::publishEvent,
                errorHandler,
                partitionFactory,
                metricsEventPublisher);

        this.lowWatermarkStampPublisher = new LowWatermarkStampPublisher(connectorConfig,
                spannerEventDispatcher, this::onError, taskSyncContextHolder);

        TaskPartitionRebalancer taskPartitionRebalancer = leaderRebalanceStrategy.equals(LeaderRebalanceStrategy.EQUAL_SHARING)
                ? new TaskPartitionEqualSharingRebalancer()
                : new TaskPartitionGreedyLeaderRebalancer();

        this.leaderAction = new LeaderAction(taskSyncContextHolder, kafkaAdminService, leaderService,
                taskPartitionRebalancer, taskSyncPublisher, this::onError);

        this.taskSyncEventListener = new TaskSyncEventListener(task.getTaskUid(), taskSyncTopic, syncEventConsumerFactory,
                true, this::onError);

        this.rebalancingEventListener = new RebalancingEventListener(task, connectorName, rebalancingTopic,
                connectorConfig.rebalancingTaskWaitingTimeout(), rebalancingConsumerFactory, this::onError);

        this.taskStateChangeEventHandler = new TaskStateChangeEventHandler(taskSyncContextHolder, taskSyncPublisher,
                changeStream, partitionFactory, this::onFinish, connectorConfig, this::onError);

        this.rebalanceHandler = new RebalanceHandler(taskSyncContextHolder, taskSyncPublisher,
                leaderAction, lowWatermarkStampPublisher);

        this.syncEventHandler = new SyncEventHandler(taskSyncContextHolder,
                taskSyncPublisher, this::publishEvent);

        final LowWatermarkCalculator lowWatermarkCalculator = new LowWatermarkCalculator(connectorConfig, taskSyncContextHolder, partitionOffsetProvider);

        this.lowWatermarkCalculationJob = new LowWatermarkCalculationJob(connectorConfig, this::onError, lowWatermarkCalculator,
                lowWatermarkHolder);

        this.taskStateChangeEventProcessor = new TaskStateChangeEventProcessor(connectorConfig.taskStateChangeEventQueueCapacity(),
                taskSyncContextHolder, taskStateChangeEventHandler, this::onError, metricsEventPublisher);

    }

    public synchronized void init() {
        try {

            this.taskSyncContextHolder.init(TaskSyncContext.getInitialContext(this.task.getTaskUid(), connectorConfig));

            this.rebalanceHandler.init();

            this.taskSyncEventListener.subscribe(syncEventHandler::updateCurrentOffset);

            this.taskSyncEventListener.subscribe(syncEventHandler::processPreviousStates);

            this.taskSyncEventListener.subscribe(syncEventHandler::process);

            this.taskSyncEventListener.start();

            this.taskSyncContextHolder.awaitInitialization();

            this.rebalancingEventListener
                    .listen(metadata -> rebalanceHandler.process(metadata.isLeader(), metadata.getConsumerId(), metadata.getRebalanceGenerationId()));

            this.taskSyncContextHolder.awaitNewEpoch();

            this.lowWatermarkCalculationJob.start();

            this.schemaRegistry.init(taskSyncContextHolder.get().getDatabaseSchemaTimestamp());

            this.taskStateChangeEventProcessor.startProcessing();

            this.taskSyncContextHolder.update(context -> context.toBuilder().initialized(true).build());

        }
        catch (Throwable ex) {
            LOGGER.error("Exception during SynchronizationTaskContext starting", ex);
            this.onError(ex);
        }
    }

    public synchronized void destroy() {

        try {
            this.taskSyncEventListener.shutdown();
            this.rebalancingEventListener.shutdown();

            this.taskStateChangeEventProcessor.stopProcessing();

            this.lowWatermarkCalculationJob.stop();

            this.rebalanceHandler.destroy();
            this.taskSyncPublisher.close();
        }
        catch (Throwable ex) {
            LOGGER.warn("Exception during sync context destroying", ex);
        }

    }

    public void publishEvent(TaskStateChangeEvent event) throws InterruptedException {
        LoggerUtils.debug(LOGGER, "publishEvent: type: {}, event: {}", event.getClass().getSimpleName(), event);

        this.taskStateChangeEventProcessor.processEvent(event);
    }

    private void onError(Throwable throwable) {
        this.errorHandler.setProducerThrowable(throwable);
    }

    private void onFinish() {
        this.finishingHandler.run();
    }
}
