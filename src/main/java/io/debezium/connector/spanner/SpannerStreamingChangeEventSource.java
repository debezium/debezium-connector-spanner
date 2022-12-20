/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;
import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.connector.spanner.context.offset.SpannerOffsetContextFactory;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.model.Mod;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.model.event.ChildPartitionsEvent;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.event.FinishPartitionEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;
import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.db.stream.PartitionEventListener;
import io.debezium.connector.spanner.exception.FinishingPartitionTimeout;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.ChildPartitionsMetricEvent;
import io.debezium.connector.spanner.processor.SourceRecordUtils;
import io.debezium.connector.spanner.processor.SpannerChangeRecordEmitter;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Clock;

/**
 * Processes all types of Spanner Stream Events
 */
public class SpannerStreamingChangeEventSource implements CommittingRecordsStreamingChangeEventSource<SpannerPartition, SpannerOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerStreamingChangeEventSource.class);

    private static final StuckPartitionStrategy STUCK_PARTITION_STRATEGY = StuckPartitionStrategy.ESCALATE;

    private static final Duration FINISHING_PARTITION_TIMEOUT = Duration.ofSeconds(60);

    private final FinishPartitionStrategy finishPartitionStrategy;

    private final ErrorHandler errorHandler;

    private final StreamEventQueue eventQueue;

    private final MetricsEventPublisher metricsEventPublisher;

    private final ChangeStream stream;

    private final PartitionManager partitionManager;

    private final SchemaRegistry schemaRegistry;

    private final SpannerEventDispatcher spannerEventDispatcher;

    private final FinishingPartitionManager finishingPartitionManager;

    private final FinishPartitionWatchDog finishPartitionWatchDog;

    private final SpannerOffsetContextFactory offsetContextFactory;

    private volatile Thread thread;

    public SpannerStreamingChangeEventSource(ErrorHandler errorHandler,
                                             ChangeStream stream,
                                             StreamEventQueue eventQueue,
                                             MetricsEventPublisher metricsEventPublisher,
                                             PartitionManager partitionManager,
                                             SchemaRegistry schemaRegistry,
                                             SpannerEventDispatcher spannerEventDispatcher,
                                             boolean finishingAfterCommit,
                                             SpannerOffsetContextFactory offsetContextFactory) {
        this.offsetContextFactory = offsetContextFactory;
        this.errorHandler = errorHandler;
        this.eventQueue = eventQueue;
        this.metricsEventPublisher = metricsEventPublisher;
        this.stream = stream;
        this.partitionManager = partitionManager;
        this.schemaRegistry = schemaRegistry;
        this.spannerEventDispatcher = spannerEventDispatcher;
        this.finishingPartitionManager = new FinishingPartitionManager(partitionManager::updateToFinished);
        this.finishPartitionWatchDog = new FinishPartitionWatchDog(finishingPartitionManager, FINISHING_PARTITION_TIMEOUT, tokens -> {
            processFailure(new FinishingPartitionTimeout(tokens));
        });

        this.finishPartitionStrategy = finishingAfterCommit
                ? FinishPartitionStrategy.AFTER_COMMIT
                : FinishPartitionStrategy.AFTER_STREAMING_FINISH;
    }

    @Override
    public void execute(ChangeEventSourceContext context,
                        SpannerPartition partition,
                        SpannerOffsetContext offsetContext)
            throws InterruptedException {

        LOGGER.info("Starting streaming...");
        try {

            startProcessing(context);

            stream.run(context::isRunning, eventQueue::put, new PartitionEventListener() {
                @Override
                public void onRun(Partition partition) throws InterruptedException {
                    finishingPartitionManager.registerPartition(partition.getToken());
                    partitionManager.updateToRunning(partition.getToken());
                }

                @Override
                public void onFinish(Partition partition) {
                    LOGGER.info("Partition onFinish: {}", partition.getToken());
                }

                @Override
                public void onException(Partition partition, Exception exception) throws InterruptedException {
                    LOGGER.error("Try to stream again from partition {} after exception {}", partition.getToken(),
                            exception.getMessage());

                    partitionManager.updateToReadyForStreaming(partition.getToken());
                }

                @Override
                public boolean onStuckPartition(String token) throws InterruptedException {
                    if (STUCK_PARTITION_STRATEGY.equals(StuckPartitionStrategy.REPEAT_STREAMING)) {
                        LOGGER.warn("Try to requery partition {}", token);
                        partitionManager.updateToReadyForStreaming(token);
                    }
                    else if (STUCK_PARTITION_STRATEGY.equals(StuckPartitionStrategy.ESCALATE)) {
                        return true;
                    }
                    return false;
                }
            });

        }
        catch (InterruptedException ex) {
            LOGGER.info("Continue to stop streaming...");
        }
        catch (Exception ex) {
            processFailure(ex);
        }
        finally {
            LOGGER.info("Stopping streaming...");

            finishPartitionWatchDog.stop();

            if (thread != null) {
                thread.interrupt();
            }
        }

    }

    private void startProcessing(ChangeEventSourceContext context) {
        thread = new Thread(() -> {
            try {
                while (context.isRunning()) {
                    ChangeStreamEvent event = eventQueue.take();

                    if (event instanceof DataChangeEvent) {

                        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;

                        processDataChangeEvent(dataChangeEvent);
                    }
                    else if (event instanceof HeartbeatEvent) {

                        HeartbeatEvent heartbeatEvent = (HeartbeatEvent) event;

                        processHeartBeatEvent(heartbeatEvent);
                    }
                    else if (event instanceof ChildPartitionsEvent) {

                        ChildPartitionsEvent childPartitionsEvent = (ChildPartitionsEvent) event;

                        processChildPartitionsEvent(childPartitionsEvent);
                    }
                    else if (event instanceof FinishPartitionEvent) {
                        LOGGER.info("Received FinishPartitionEvent for partition {}", event.getMetadata().getPartitionToken());
                        if (finishPartitionStrategy.equals(FinishPartitionStrategy.AFTER_COMMIT)) {
                            this.finishingPartitionManager.onPartitionFinishEvent(event.getMetadata().getPartitionToken());
                        }
                        else if (finishPartitionStrategy.equals(FinishPartitionStrategy.AFTER_STREAMING_FINISH)) {
                            finishingPartitionManager.forceFinish(event.getMetadata().getPartitionToken());
                        }
                    }
                    else {
                        // ignore event
                    }
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Exception ex) {
                processFailure(ex);
            }
        }, "SpannerConnector-SpannerStreamingChangeEventSource");

        thread.start();
    }

    @VisibleForTesting
    void processDataChangeEvent(DataChangeEvent event) throws InterruptedException {
        TableId tableId = TableId.getTableId(event.getTableName());

        schemaRegistry.checkSchema(tableId, event.getCommitTimestamp(), event.getRowType());

        SpannerPartition partition = new SpannerPartition(event.getPartitionToken());

        for (Mod mod : event.getMods()) {
            String recordUid = UUID.randomUUID().toString();

            SpannerOffsetContext offsetContext = offsetContextFactory.getOffsetContextFromDataChangeEvent(mod.getModNumber(), event);

            finishingPartitionManager.newRecord(partition.getValue(), recordUid);

            boolean dispatched = spannerEventDispatcher.dispatchDataChangeEvent(partition, tableId,
                    new SpannerChangeRecordEmitter(recordUid, event.getModType(), mod, partition, offsetContext,
                            Clock.SYSTEM));
            if (dispatched) {
                LOGGER.debug("DataChangeEvent has been dispatched form table {} with modification: {}, offset{}, event: {}", tableId.getTableName(), mod,
                        offsetContext.getOffset(), event);
            }
            else {
                LOGGER.info("DataChangeEvent has not been dispatched form table {} with modification: {}", tableId.getTableName(), mod);
            }
        }

    }

    private void processHeartBeatEvent(HeartbeatEvent event) throws InterruptedException {
        SpannerOffsetContext offsetContext = offsetContextFactory.getOffsetContextFromHeartbeatEvent(event);

        SpannerPartition partition = new SpannerPartition(event.getMetadata().getPartitionToken());

        spannerEventDispatcher.alwaysDispatchHeartbeatEvent(partition, offsetContext);

        LOGGER.debug("Dispatching heartbeat for event {} with partition {} and offset {}", event, partition, offsetContext.getOffset());
    }

    private void processChildPartitionsEvent(ChildPartitionsEvent event) throws InterruptedException {
        LOGGER.info("Received ChildPartitionsEvent: {}", event);

        List<Partition> partitions = event.getChildPartitions().stream().map(childPartition -> {
            Timestamp startTimeStamp = event.getStartTimestamp();
            return Partition.builder()
                    .token(childPartition.getToken())
                    .parentTokens(childPartition.getParentTokens())
                    .startTimestamp(startTimeStamp)
                    .endTimestamp(event.getMetadata().getPartitionEndTimestamp())
                    .originPartitionToken(event.getMetadata().getPartitionToken())
                    .build();
        }).collect(Collectors.toList());

        this.partitionManager.newChildPartitions(partitions);

        metricsEventPublisher.publishMetricEvent(new ChildPartitionsMetricEvent(event.getChildPartitions().size()));
    }

    private void processFailure(Exception ex) {
        errorHandler.setProducerThrowable(ex);
    }

    @Override
    public void commitRecords(List<SourceRecord> records) throws InterruptedException {

        if (!finishPartitionStrategy.equals(FinishPartitionStrategy.AFTER_COMMIT)) {
            return;
        }

        for (SourceRecord sourceRecord : records) {
            String token = SourceRecordUtils.extractToken(sourceRecord);
            String recordUid = SourceRecordUtils.extractRecordUid(sourceRecord);

            if (token == null || recordUid == null) {
                continue;
            }

            this.finishingPartitionManager.commitRecord(token, recordUid);
        }
    }

}
