/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;
import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.connector.spanner.context.offset.SpannerOffsetContextFactory;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.model.ChildPartition;
import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.Mod;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.model.event.ChildPartitionsEvent;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.event.FinishPartitionEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEventEvent;
import io.debezium.connector.spanner.db.model.event.PartitionStartEvent;
import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.db.stream.PartitionEventListener;
import io.debezium.connector.spanner.exception.FinishingPartitionTimeout;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.ChildPartitionsMetricEvent;
import io.debezium.connector.spanner.processor.SpannerChangeRecordEmitter;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.OffsetActivityMonitorService;
import io.debezium.util.Clock;

/**
 * Processes all types of Spanner Stream Events
 */
public class SpannerStreamingChangeEventSource implements CommittingRecordsStreamingChangeEventSource<SpannerPartition, SpannerOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerStreamingChangeEventSource.class);

    private static final StuckPartitionStrategy STUCK_PARTITION_STRATEGY = StuckPartitionStrategy.ESCALATE;

    private static final Duration FINISHING_PARTITION_TIMEOUT = Duration.ofSeconds(300);

    private static final long INITIAL_TOKEN_BATCH_SIZE = 200;

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

    private final SpannerConnectorConfig connectorConfig;

    private final OffsetActivityMonitorService offsetActivityMonitorService;

    private OffsetActivityMonitor<SpannerPartition, SpannerOffsetContext> offsetActivityMonitor;

    private volatile Thread thread;

    public SpannerStreamingChangeEventSource(SpannerConnectorConfig connectorConfig,
                                             ErrorHandler errorHandler,
                                             ChangeStream stream,
                                             StreamEventQueue eventQueue,
                                             MetricsEventPublisher metricsEventPublisher,
                                             PartitionManager partitionManager,
                                             SchemaRegistry schemaRegistry,
                                             SpannerEventDispatcher spannerEventDispatcher,
                                             boolean finishingAfterCommit,
                                             SpannerOffsetContextFactory offsetContextFactory) {
        this.connectorConfig = connectorConfig;
        this.offsetContextFactory = offsetContextFactory;
        this.errorHandler = errorHandler;
        this.eventQueue = eventQueue;
        this.metricsEventPublisher = metricsEventPublisher;
        this.stream = stream;
        this.partitionManager = partitionManager;
        this.schemaRegistry = schemaRegistry;
        this.spannerEventDispatcher = spannerEventDispatcher;
        this.finishingPartitionManager = new FinishingPartitionManager(connectorConfig, partitionManager::updateToFinished);
        this.finishPartitionWatchDog = new FinishPartitionWatchDog(finishingPartitionManager, FINISHING_PARTITION_TIMEOUT, tokens -> {
            processFailure(new FinishingPartitionTimeout(tokens));
        });

        this.finishPartitionStrategy = finishingAfterCommit
                ? FinishPartitionStrategy.AFTER_COMMIT
                : FinishPartitionStrategy.AFTER_STREAMING_FINISH;
        this.offsetActivityMonitorService = OffsetActivityMonitorService.lookup(connectorConfig.getServiceRegistry());
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

                @Override
                public void onWindowAdvanced(Partition partition, Timestamp windowEnd, String lastBoundaryRecordSequence) throws InterruptedException {
                    partitionManager.updateProcessedTimestamp(partition.getToken(), windowEnd, lastBoundaryRecordSequence);
                }

                @Override
                public void onMoveIn(Partition partition, Timestamp commitTimestamp, String recordSequence, List<String> sourcePartitionTokens)
                        throws InterruptedException {
                    if (!connectorConfig.isMutablePartitionOrderingEnabled()) {
                        LOGGER.debug("Mutable ordering disabled; ignoring MoveIn pause for partition {}", partition.getToken());
                        return;
                    }
                    LOGGER.info("Partition onMoveIn: {}, commitTimestamp={}, recordSequence={}, sources={}",
                            partition.getToken(), commitTimestamp, recordSequence, sourcePartitionTokens);
                    partitionManager.notifyMoveIn(partition.getToken(), commitTimestamp, recordSequence, sourcePartitionTokens);
                }

                @Override
                public void onMoveInPublishOnly(Partition partition, Timestamp commitTimestamp, String recordSequence,
                                                List<String> sourcePartitionTokens, boolean isFirstMoveIn)
                        throws InterruptedException {
                    if (!connectorConfig.isMutablePartitionOrderingEnabled()) {
                        return;
                    }
                    LOGGER.info("Partition onMoveInPublishOnly (buffer-gate): {}, commitTimestamp={}, recordSequence={}, sources={}, isFirst={}",
                            partition.getToken(), commitTimestamp, recordSequence, sourcePartitionTokens, isFirstMoveIn);
                    partitionManager.publishMoveInStateOnly(
                            partition.getToken(), commitTimestamp, recordSequence, sourcePartitionTokens, isFirstMoveIn);
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
                List<ChildPartitionsEvent> initialChildPartitionTokens = new ArrayList<ChildPartitionsEvent>();
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

                        if (!childPartitionsEvent.getChildPartitions().isEmpty() &&
                                childPartitionsEvent.getChildPartitions().get(0).getParentTokens().contains(InitialPartition.PARTITION_TOKEN)) {
                            LOGGER.info("Batching child partition event {}", childPartitionsEvent);
                            initialChildPartitionTokens.add(childPartitionsEvent);
                        }
                        else {
                            processChildPartitionsEvent(Collections.singletonList(childPartitionsEvent));
                        }

                    }
                    else if (event instanceof FinishPartitionEvent) {

                        if (!initialChildPartitionTokens.isEmpty()) {
                            LOGGER.info("Received FinishPartitionEvent {}, so clearing initial tokens", event.getMetadata().getPartitionToken());
                            if (initialChildPartitionTokens.size() <= INITIAL_TOKEN_BATCH_SIZE) {
                                processChildPartitionsEvent(initialChildPartitionTokens);
                            }
                            else {
                                List<ChildPartitionsEvent> batchedInitialTokens = new ArrayList<>();
                                // Make sure to send the child partitions downstream in batches.
                                for (ChildPartitionsEvent childPartitionsEvent : initialChildPartitionTokens) {
                                    batchedInitialTokens.add(childPartitionsEvent);
                                    if (batchedInitialTokens.size() >= INITIAL_TOKEN_BATCH_SIZE) {
                                        processChildPartitionsEvent(batchedInitialTokens);
                                        batchedInitialTokens.clear();
                                    }

                                }
                                if (!batchedInitialTokens.isEmpty()) {
                                    processChildPartitionsEvent(batchedInitialTokens);
                                }
                            }
                            initialChildPartitionTokens.clear();
                        }

                        LOGGER.info("Received FinishPartitionEvent for partition {}", event.getMetadata().getPartitionToken());

                        if (finishPartitionStrategy.equals(FinishPartitionStrategy.AFTER_COMMIT)) {
                            this.finishingPartitionManager.onPartitionFinishEvent(event.getMetadata().getPartitionToken());
                        }
                        else if (finishPartitionStrategy.equals(FinishPartitionStrategy.AFTER_STREAMING_FINISH)) {
                            finishingPartitionManager.forceFinish(event.getMetadata().getPartitionToken());
                        }
                    }
                    else if (event instanceof PartitionStartEvent) {

                        PartitionStartEvent partitionStartEvent = (PartitionStartEvent) event;

                        processPartitionStartEvent(partitionStartEvent);

                    }
                    else if (event instanceof PartitionEventEvent) {

                        PartitionEventEvent partitionEventEvent = (PartitionEventEvent) event;

                        processPartitionEventEvent(partitionEventEvent);

                    }
                    else {
                        // ignore event
                    }
                }
            }
            catch (InterruptedException e) {
                LOGGER.info("Interrupting SpannerConnector-SpannerStreamingChangeEventSource task");
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
            SpannerOffsetContext offsetContext = offsetContextFactory.getOffsetContextFromDataChangeEvent(mod.getModNumber(), event);

            String recordUid = this.finishingPartitionManager.newRecord(event.getPartitionToken());

            boolean dispatched = spannerEventDispatcher.dispatchDataChangeEvent(partition, tableId,
                    new SpannerChangeRecordEmitter(recordUid, event.getModType(), mod, partition, offsetContext,
                            Clock.SYSTEM, connectorConfig));

            pulseOffsetActivityMonitor(partition, offsetContext);

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

        pulseOffsetActivityMonitor(partition, offsetContext);

        LOGGER.debug("Dispatching heartbeat for event {} with partition {} and offset {}", event, partition, offsetContext.getOffset());
    }

    private void processChildPartitionsEvent(List<ChildPartitionsEvent> events) throws InterruptedException {
        List<Partition> childPartitionsToSend = new ArrayList<>();
        for (ChildPartitionsEvent event : events) {
            LOGGER.info("Received ChildPartitionsEvent in StreamingChangeEventSource: {}", event);
            if (event.getChildPartitions().size() > 1) {
                LOGGER.info("A split event occurred {}", event);
            }
            else {
                LOGGER.info("A move or merge event occurred {}", event);

            }
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
            childPartitionsToSend.addAll(partitions);
            metricsEventPublisher.publishMetricEvent(new ChildPartitionsMetricEvent(event.getChildPartitions().size()));
        }
        LOGGER.info("Sending List<Partition> to PartitionManager: {}", childPartitionsToSend);
        this.partitionManager.newChildPartitions(childPartitionsToSend);
    }

    /**
     * Signals the offset activity monitor service that an event's offsets have been processed.
     * <p>
     * Invoked on the event-processing thread; the monitor is registered on the coordinator
     * thread before streaming is started, so it is safely published by the thread start.
     */
    private void pulseOffsetActivityMonitor(SpannerPartition partition, SpannerOffsetContext offsetContext) {
        offsetActivityMonitorService.pulse(partition, offsetContext);
    }

    private void processPartitionStartEvent(PartitionStartEvent event) throws InterruptedException {
        LOGGER.info("Received PartitionStartEvent: schedulingPartitions={} from sourceToken={}",
                event.getPartitionTokens(), event.getMetadata().getPartitionToken());
        // Mutable key range: destination partitions are scheduled with EMPTY parents.
        // PartitionStartRecord has no parent_partition_tokens field (unlike immutable child_partitions_record).
        // Source partition A continues streaming after a partial move-out — it never emits PartitionEndRecord —
        // so gating D on A.state == FINISHED would deadlock permanently.
        // The A→D dependency is established later when D's own stream hits PartitionEventRecord::MoveInEvent.
        List<ChildPartition> children = event.getPartitionTokens().stream()
                .map(t -> new ChildPartition(t, Set.of()))
                .collect(Collectors.toList());
        ChildPartitionsEvent synthetic = new ChildPartitionsEvent(
                event.getStartTimestamp(), event.getRecordSequence(), children, event.getMetadata());
        processChildPartitionsEvent(Collections.singletonList(synthetic));
    }

    private void processPartitionEventEvent(PartitionEventEvent event) throws InterruptedException {
        LOGGER.info("Received PartitionEventEvent: token={}, sources={}, destinations={}",
                event.getPartitionToken(), event.getSourcePartitions(), event.getDestinationPartitions());
        if (!event.getDestinationPartitions().isEmpty()) {
            processMoveOutEvent(event); // MoveOut side — gated
        }
        // MoveIn (sourcePartitions non-empty): handled by PartitionEventListener#onMoveIn,
        // triggered directly from the partition streaming loop when the query is paused.
    }

    private void processMoveOutEvent(PartitionEventEvent event) throws InterruptedException {
        if (!connectorConfig.isMutablePartitionOrderingEnabled()) {
            LOGGER.debug("Mutable ordering disabled; skipping MoveOut notification for partition {}",
                    event.getPartitionToken());
            return;
        }
        partitionManager.notifyMoveOut(
                event.getPartitionToken(),
                event.getCommitTimestamp(),
                event.getDestinationPartitions());

        // A partition under heavy MoveOut churn can emit thousands of these events per window with
        // no DataChangeEvent/HeartbeatEvent in between (see SpannerChangeStreamService's per-window
        // dataEvents/heartbeatEvents counters). Only those two event types previously advanced the
        // Kafka Connect-committed offset, so this partition's offset — and the low watermark, which
        // falls back to it when no fresher offset exists — stayed frozen at the window start despite
        // real, continuous progress, only catching up once the outer window boundary closed (up to
        // mutable.window.minutes late). Dispatch the MoveOut event's own commit timestamp the same
        // way a heartbeat is dispatched so this progress is reflected immediately.
        SpannerOffsetContext offsetContext = offsetContextFactory.getOffsetContextFromPartitionEventEvent(event);
        SpannerPartition partition = new SpannerPartition(event.getPartitionToken());
        spannerEventDispatcher.alwaysDispatchHeartbeatEvent(partition, offsetContext);
    }

    private void processFailure(Exception ex) {
        errorHandler.setProducerThrowable(ex);
    }

    @Override
    public Optional<OffsetActivityMonitor<SpannerPartition, SpannerOffsetContext>> getOffsetActivityMonitor() {
        if (offsetActivityMonitor == null) {
            offsetActivityMonitor = new SpannerOffsetActivityMonitor(connectorConfig.getOffsetActivityMonitorInterval());
        }
        return Optional.of(offsetActivityMonitor);
    }

    @Override
    public void commitRecords(List<CommittedRecord> records) throws InterruptedException {

        if (!finishPartitionStrategy.equals(FinishPartitionStrategy.AFTER_COMMIT)) {
            return;
        }

        for (CommittedRecord record : records) {
            this.finishingPartitionManager.commitRecord(record.token(), record.recordUid());
        }
    }

}
