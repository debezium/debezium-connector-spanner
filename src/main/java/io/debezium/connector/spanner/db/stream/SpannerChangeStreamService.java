/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.dao.ChangeStreamDao;
import io.debezium.connector.spanner.db.dao.ChangeStreamResultSet;
import io.debezium.connector.spanner.db.dao.ChangeStreamResultSetMetadata;
import io.debezium.connector.spanner.db.mapper.ChangeStreamRecordMapper;
import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.model.event.ChildPartitionsEvent;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.event.FinishPartitionEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEndEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEventEvent;
import io.debezium.connector.spanner.db.model.event.RecordSequenceUtils;
import io.debezium.connector.spanner.db.stream.exception.ChangeStreamException;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.DelayChangeStreamEventsMetricEvent;
import io.debezium.connector.spanner.metrics.event.MoveInLatencyMetricEvent;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * This class queries the change stream, sends child partitions to SynchronizedPartitionManager,
 * and updates the last commit timestamp for each partition.
 */
public class SpannerChangeStreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerChangeStreamService.class);

    public static final Duration DEFAULT_HEARTBEAT_LAG_WARN_THRESHOLD = Duration.ofSeconds(60);

    private final ChangeStreamDao changeStreamDao;
    private final ChangeStreamRecordMapper changeStreamRecordMapper;

    private final Duration heartbeatMillis;
    private final MetricsEventPublisher metricsEventPublisher;
    private final String taskUid;
    private final Duration windowDuration;
    private final boolean mutablePartitionOrderingEnabled;
    private final Duration heartbeatLagWarnThreshold;

    /**
     * Non-blocking supplier of the live {@link TaskSyncContext} snapshot used by
     * {@link MoveInBufferGate#isGateOpen()}.  May be {@code null} when the buffer-gate
     * optimisation is disabled (immutable change streams, or ordering disabled).
     */
    private final Supplier<TaskSyncContext> taskSyncContextSupplier;

    /** Maximum events to buffer in a {@link MoveInBufferGate} before falling back. */
    private final int moveInBufferMaxEvents;

    /**
     * Polling interval (ms) for the post-window spin-wait that checks whether the gate
     * has opened after the Spanner result set was exhausted.
     */
    private final int moveInGateCheckIntervalMs;

    /**
     * Maximum time (ms) the post-window spin-wait may block waiting for the MoveIn gate
     * to drain before falling back to the close/reopen path. Configured via
     * {@code gcp.spanner.mutable.move.in.gate.timeout.ms}.
     */
    private final int moveInGateTimeoutMs;

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher) {
        this(taskUid, changeStreamDao, changeStreamRecordMapper, heartbeatMillis, metricsEventPublisher, 20);
    }

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher, int windowMinutes) {
        this(taskUid, changeStreamDao, changeStreamRecordMapper, heartbeatMillis, metricsEventPublisher,
                windowMinutes, MutableStreamOptions.withDefaults());
    }

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher, int windowMinutes,
                                      boolean mutablePartitionOrderingEnabled) {
        this(taskUid, changeStreamDao, changeStreamRecordMapper, heartbeatMillis, metricsEventPublisher, windowMinutes,
                mutablePartitionOrderingEnabled, DEFAULT_HEARTBEAT_LAG_WARN_THRESHOLD);
    }

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher, int windowMinutes,
                                      boolean mutablePartitionOrderingEnabled, Duration heartbeatLagWarnThreshold) {
        this(taskUid, changeStreamDao, changeStreamRecordMapper, heartbeatMillis, metricsEventPublisher, windowMinutes,
                mutablePartitionOrderingEnabled ? MutableStreamOptions.withDefaults() : MutableStreamOptions.orderingDisabled(),
                heartbeatLagWarnThreshold);
    }

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher, int windowMinutes,
                                      MutableStreamOptions options) {
        this(taskUid, changeStreamDao, changeStreamRecordMapper, heartbeatMillis, metricsEventPublisher, windowMinutes,
                options, DEFAULT_HEARTBEAT_LAG_WARN_THRESHOLD);
    }

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher, int windowMinutes,
                                      MutableStreamOptions options, Duration heartbeatLagWarnThreshold) {
        this.changeStreamDao = changeStreamDao;
        this.changeStreamRecordMapper = changeStreamRecordMapper;
        this.heartbeatMillis = heartbeatMillis;
        this.metricsEventPublisher = metricsEventPublisher;
        this.taskUid = taskUid;
        this.windowDuration = Duration.ofMinutes(windowMinutes);
        this.mutablePartitionOrderingEnabled = options.isOrderingEnabled();
        this.heartbeatLagWarnThreshold = heartbeatLagWarnThreshold;
        this.taskSyncContextSupplier = options.getTaskSyncContextSupplier();
        this.moveInBufferMaxEvents = options.getBufferMaxEvents();
        this.moveInGateCheckIntervalMs = options.getGateCheckIntervalMs();
        this.moveInGateTimeoutMs = options.getGateTimeoutMs();
    }

    public boolean isMutableKeyRange() {
        return changeStreamDao.isMutableKeyRange();
    }

    public void getEvents(Partition partition, ChangeStreamEventConsumer changeStreamEventConsumer,
                          PartitionEventListener partitionEventListener)
            throws InterruptedException, Exception {
        if (changeStreamDao.isMutableKeyRange()) {
            getEventsMutable(partition, changeStreamEventConsumer, partitionEventListener);
        }
        else {
            getEventsImmutable(partition, changeStreamEventConsumer, partitionEventListener);
        }
    }

    private void getEventsImmutable(Partition partition, ChangeStreamEventConsumer changeStreamEventConsumer,
                                    PartitionEventListener partitionEventListener)
            throws InterruptedException, Exception {
        final String token = partition.getToken();

        partitionEventListener.onRun(partition);

        LOGGER.info("Task: {}, Streaming {} from {} to {}", taskUid, token, partition.getStartTimestamp(), partition.getEndTimestamp());
        boolean receivedChildPartitions = false;
        try (ChangeStreamResultSet resultSet = changeStreamDao.streamQuery(token, partition.getStartTimestamp(),
                partition.getEndTimestamp(), heartbeatMillis.toMillis())) {

            long start = now();
            while (resultSet.next()) {
                long delay = now() - start;

                List<ChangeStreamEvent> events = changeStreamRecordMapper.toChangeStreamEvents(
                        partition,
                        resultSet, resultSet.getMetadata());
                LOGGER.debug("Task: {}, Events receive from stream: {}", taskUid, events);

                for (ChangeStreamEvent event : events) {
                    if (event instanceof ChildPartitionsEvent) {
                        receivedChildPartitions = true;
                    }
                }

                if (!events.isEmpty() && (events.get(0) instanceof HeartbeatEvent)) {
                    var heartbeatEvent = (HeartbeatEvent) events.get(0);
                    long heartbeatLag = System.currentTimeMillis() - heartbeatEvent.getRecordTimestamp().toSqlTimestamp().toInstant().toEpochMilli();
                    if (heartbeatLag > heartbeatLagWarnThreshold.toMillis()) {
                        LOGGER.warn("Task: {}, heartbeat has very old timestamp, lag: {}, token: {}, event: {}", taskUid, heartbeatLag,
                                heartbeatEvent.getMetadata().getPartitionToken(),
                                heartbeatEvent);
                    }
                }

                processEvents(partition, events, changeStreamEventConsumer);

                if (!events.isEmpty() && !(events.get(0) instanceof HeartbeatEvent)) {
                    metricsEventPublisher.publishMetricEvent(new DelayChangeStreamEventsMetricEvent((int) delay));
                }

                start = now();
            }
        }
        catch (InterruptedException ex) {
            LOGGER.info("task {}, Interrupting streaming partition task with token {}", this.taskUid, partition.getToken());
            Thread.currentThread().interrupt();
            return;
        }

        boolean reachedEnd = receivedChildPartitions || partition.getEndTimestamp() != null;

        if (!reachedEnd) {
            LOGGER.error(
                    "Task: {}, Partition {} stream ended without delivering child partition records! Retrying partition stream.",
                    taskUid, token);
            throw new ChangeStreamException(
                    "Partition " + token + " stream ended without child partitions. Retrying partition stream.");
        }

        partitionEventListener.onFinish(partition);
        LOGGER.info("Task {}, Finished consuming partition {}", taskUid, partition);

        changeStreamEventConsumer.acceptChangeStreamEvent(new FinishPartitionEvent(partition));
    }

    private void getEventsMutable(Partition partition, ChangeStreamEventConsumer changeStreamEventConsumer,
                                  PartitionEventListener partitionEventListener)
            throws InterruptedException, Exception {
        final String token = partition.getToken();

        partitionEventListener.onRun(partition);

        LOGGER.info("Task: {}, Streaming mutable partition {} from {} to {}", taskUid, token,
                partition.getStartTimestamp(), partition.getEndTimestamp());

        Timestamp partitionEndTimestamp = partition.getEndTimestamp();

        Timestamp processedTimestamp = partition.getStartTimestamp();
        String lastBoundaryRecordSequence = partition.getLastBoundaryRecordSequence();
        boolean isPartitionEnded = false;

        // Overflow / interrupt-with-gate fallback: set when buffer capacity is exceeded
        // or when the streaming thread is interrupted while a gate is active.
        boolean isPartitionMoveInEvent = false;
        // Interrupt flag is deferred until AFTER onMoveIn() completes. onMoveIn() eventually
        // calls BlockingQueue.put() via lockInterruptibly(), which throws InterruptedException
        // immediately if the flag is already set — causing the MoveIn notification to be lost.
        boolean restoreInterruptAfterMoveIn = false;
        PartitionEventEvent moveInEvent = null;
        ChangeStreamResultSetMetadata moveInMetadata = null;
        // All source tokens accumulated across every MoveIn seen by the active gate.
        // Non-null only in the overflow / interrupt-with-gate paths; used instead of
        // moveInEvent.getSourcePartitions() so MoveInStateUpdateOperation records every
        // source, not only those of the first MoveIn event.
        List<String> moveInAllSources = null;

        // Buffer gate: non-null while the streaming thread is gating on a MoveIn event.
        // Persists across window iterations so the gate waits between result sets if needed.
        MoveInBufferGate gate = null;
        // Whether the current gate is for the first MoveIn in this buffer sequence
        // (used to set isFirstMoveIn correctly for subsequent MoveIn events on the same gate).
        boolean gateIsFirst = true;

        while (!isPartitionEnded && !isPartitionMoveInEvent
                && (partitionEndTimestamp == null || isBeforeOrEqual(processedTimestamp, partitionEndTimestamp))) {
            Timestamp endTimestamp = partitionEndTimestamp == null
                    ? addMinutes(processedTimestamp, windowDuration)
                    : minTimestamp(partitionEndTimestamp, addMinutes(processedTimestamp, windowDuration));
            String newBoundaryRecordSequence = null;

            // Diagnostic counters for this window iteration only, to make it possible to see
            // directly from INFO-level logs whether Spanner ever emitted anything (heartbeat or
            // data) for this partition during the window, rather than inferring it indirectly
            // from offset timestamps after the fact. See the window-closed summary log below.
            long windowStartWallMs = System.currentTimeMillis();
            int dataEventCountInWindow = 0;
            int heartbeatEventCountInWindow = 0;
            int partitionEventCountInWindow = 0;
            long lastEventWallMs = -1;

            try (ChangeStreamResultSet resultSet = changeStreamDao.streamQuery(token, processedTimestamp,
                    endTimestamp, heartbeatMillis.toMillis())) {

                long start = now();
                while (resultSet.next()) {
                    long delay = now() - start;

                    ChangeStreamResultSetMetadata metadata = resultSet.getMetadata();
                    List<ChangeStreamEvent> rawEvents = changeStreamRecordMapper.toChangeStreamEvents(
                            partition,
                            resultSet, metadata);
                    LOGGER.debug("Task: {}, Events receive from mutable stream: {}", taskUid, rawEvents);

                    List<ChangeStreamEvent> events = filterBoundaryDuplicates(rawEvents, processedTimestamp, lastBoundaryRecordSequence);

                    if (!events.isEmpty()) {
                        lastEventWallMs = System.currentTimeMillis();
                        for (ChangeStreamEvent event : events) {
                            if (event instanceof HeartbeatEvent) {
                                heartbeatEventCountInWindow++;
                            }
                            else if (event instanceof DataChangeEvent) {
                                dataEventCountInWindow++;
                            }
                            else if (event instanceof PartitionEventEvent) {
                                // MoveIn/MoveOut split notifications. A source partition under heavy
                                // MoveOut churn can emit thousands of these with zero DataChangeEvent
                                // or HeartbeatEvent in between — counting them separately is what
                                // revealed that "dataEvents=0" did not mean the partition was idle.
                                partitionEventCountInWindow++;
                            }
                        }
                    }

                    if (!events.isEmpty() && (events.get(0) instanceof HeartbeatEvent)) {
                        var heartbeatEvent = (HeartbeatEvent) events.get(0);
                        long heartbeatLag = System.currentTimeMillis() - heartbeatEvent.getRecordTimestamp().toSqlTimestamp().toInstant().toEpochMilli();
                        if (heartbeatLag > heartbeatLagWarnThreshold.toMillis()) {
                            LOGGER.warn("Task: {}, heartbeat has very old timestamp, lag: {}, token: {}, event: {}", taskUid, heartbeatLag,
                                    heartbeatEvent.getMetadata().getPartitionToken(),
                                    heartbeatEvent);
                        }
                    }

                    // Process events one by one so the buffer gate can be activated mid-batch.
                    boolean innerBreak = false;
                    for (ChangeStreamEvent event : events) {

                        // Track boundary sequence for the next window's deduplication.
                        if (endTimestamp.equals(event.getRecordTimestamp()) && event.getRecordSequence() != null) {
                            newBoundaryRecordSequence = event.getRecordSequence();
                        }

                        // Track partition end regardless of gate state.
                        if (event instanceof PartitionEndEvent) {
                            isPartitionEnded = true;
                        }

                        // MoveIn detection: activate or extend the buffer gate.
                        if (event instanceof PartitionEventEvent && mutablePartitionOrderingEnabled) {
                            PartitionEventEvent pee = (PartitionEventEvent) event;
                            if (!pee.getSourcePartitions().isEmpty()) {
                                if (taskSyncContextSupplier != null) {
                                    // Buffer-gate path: keep the gRPC connection alive.
                                    boolean isFirst;
                                    if (gate == null) {
                                        gate = new MoveInBufferGate(token, moveInBufferMaxEvents, taskSyncContextSupplier);
                                        gateIsFirst = true;
                                        isFirst = true;
                                    }
                                    else {
                                        isFirst = false;
                                    }
                                    gate.addMoveIn(pee.getCommitTimestamp(), pee.getSourcePartitions(), pee, metadata);
                                    partitionEventListener.onMoveInPublishOnly(
                                            partition, pee.getCommitTimestamp(), pee.getRecordSequence(),
                                            pee.getSourcePartitions(), gateIsFirst && isFirst);
                                    if (!isFirst) {
                                        // All subsequent MoveIn events on the same gate are not first.
                                        gateIsFirst = false;
                                    }

                                    // Drain confirmed prefix immediately: this MoveIn's source might already be confirmed.
                                    List<ChangeStreamEvent> readyAfterMoveIn = gate.drainConfirmedPrefix();
                                    if (!readyAfterMoveIn.isEmpty()) {
                                        LOGGER.info("Task {}, MoveIn gate prefix flushed immediately for partition {} ({}), flushed={}, remaining={}",
                                                taskUid, token, gate.getSourcesByTimestamp(), readyAfterMoveIn.size(), gate.size());
                                        for (ChangeStreamEvent e : readyAfterMoveIn) {
                                            changeStreamEventConsumer.acceptChangeStreamEvent(e);
                                        }
                                    }
                                    if (gate.isEmpty()) {
                                        gate = null;
                                        gateIsFirst = true;
                                    }
                                    else if (gate.isFull()) {
                                        // Overflow: fall back to existing close/reopen path.
                                        // Capture ALL accumulated sources (not just the first
                                        // MoveIn's sources) so MoveInStateUpdateOperation waits
                                        // for every source to confirm MoveOut.
                                        LOGGER.warn(
                                                "Task {}, MoveIn buffer overflow ({} events) for partition {}, falling back to close/reopen path",
                                                taskUid, gate.size(), token);
                                        isPartitionMoveInEvent = true;
                                        moveInEvent = gate.getFirstMoveInEvent();
                                        moveInMetadata = gate.getFirstMoveInMetadata();
                                        moveInAllSources = gate.getAllSources();
                                        gate = null;
                                        innerBreak = true;
                                    }
                                    continue; // Don't forward MoveIn event to consumer now; gate handles it.
                                }
                                else {
                                    // No supplier: forward the event to the consumer (matching the old
                                    // batch-processEvents behaviour), then fall back to close/reopen.
                                    changeStreamEventConsumer.acceptChangeStreamEvent(event);
                                    isPartitionMoveInEvent = true;
                                    moveInEvent = pee;
                                    moveInMetadata = metadata;
                                    gate = null;
                                    innerBreak = true;
                                }
                                if (innerBreak) {
                                    break;
                                }
                            }
                        }

                        if (innerBreak) {
                            break;
                        }

                        if (gate != null) {
                            // Gate is active: add this non-MoveIn event to the current segment.
                            gate.addDataEvent(event);

                            // Drain confirmed prefix after each event — AtomicReference read is wait-free.
                            List<ChangeStreamEvent> readyInline = gate.drainConfirmedPrefix();
                            if (!readyInline.isEmpty()) {
                                LOGGER.info("Task {}, MoveIn gate prefix flushed inline for partition {} ({}), flushed={}, remaining={}",
                                        taskUid, token, gate.getSourcesByTimestamp(), readyInline.size(), gate.size());
                                for (ChangeStreamEvent e : readyInline) {
                                    changeStreamEventConsumer.acceptChangeStreamEvent(e);
                                }
                            }
                            if (gate.isEmpty()) {
                                gate = null;
                                gateIsFirst = true;
                            }
                            else if (gate.isFull()) {
                                // Capture ALL accumulated sources before clearing the gate.
                                LOGGER.warn(
                                        "Task {}, MoveIn buffer overflow ({} events) for partition {}, falling back to close/reopen path",
                                        taskUid, gate.size(), token);
                                isPartitionMoveInEvent = true;
                                moveInEvent = gate.getFirstMoveInEvent();
                                moveInMetadata = gate.getFirstMoveInMetadata();
                                moveInAllSources = gate.getAllSources();
                                gate = null;
                                innerBreak = true;
                                break;
                            }
                        }
                        else {
                            // No active gate: forward immediately.
                            processEvents(partition, List.of(event), changeStreamEventConsumer);
                        }

                        if (isPartitionEnded) {
                            break;
                        }
                    } // for each event in batch

                    if (!events.isEmpty() && !(events.get(0) instanceof HeartbeatEvent)) {
                        metricsEventPublisher.publishMetricEvent(new DelayChangeStreamEventsMetricEvent((int) delay));
                    }

                    if (isPartitionEnded || isPartitionMoveInEvent) {
                        break;
                    }

                    start = now();
                } // while resultSet.next()
            }
            catch (InterruptedException ex) {
                LOGGER.info("task {}, Interrupting streaming mutable partition task with token {}", this.taskUid, partition.getToken());
                if (gate != null) {
                    restoreInterruptAfterMoveIn = true;
                    // Gate is active — buffered events were not yet forwarded. Transitioning
                    // to FINISHED here would lose those events permanently because FINISHED
                    // partitions are never re-streamed. Instead, transition to the MoveIn-pause
                    // state (CREATED) so the partition is re-scheduled on restart and re-reads
                    // from processedTimestamp = T1, recovering all buffered events from Spanner.
                    LOGGER.info(
                            "Task {}, MoveIn gate active at interrupt for partition {} — transitioning to MoveIn-pause to preserve unflushed events",
                            taskUid, partition.getToken());
                    if (moveInEvent == null) {
                        moveInEvent = gate.getFirstMoveInEvent();
                        moveInMetadata = gate.getFirstMoveInMetadata();
                    }
                    moveInAllSources = gate.getAllSources();
                    isPartitionMoveInEvent = true;
                    gate = null;
                }
                else {
                    gate = null;
                    Thread.currentThread().interrupt();
                }
                break;
            }

            LOGGER.info(
                    "Task: {}, Window closed for partition {}: window=[{} -> {}], dataEvents={}, heartbeatEvents={}, "
                            + "partitionEvents={}, elapsedMs={}, msSinceLastEvent={}, closedByMoveInOverflow={}, gateActive={}",
                    taskUid, token, processedTimestamp, endTimestamp, dataEventCountInWindow, heartbeatEventCountInWindow,
                    partitionEventCountInWindow,
                    System.currentTimeMillis() - windowStartWallMs,
                    lastEventWallMs < 0 ? -1 : System.currentTimeMillis() - lastEventWallMs,
                    isPartitionMoveInEvent,
                    gate != null);

            if (isPartitionMoveInEvent) {
                // Overflow fallback: break outer loop, then call onMoveIn() below.
                break;
            }

            // Post-result-set gate wait: result set ended naturally but gate is still active.
            // Spin-wait here (not inside the result-set loop) so we don't hold a Spanner
            // gRPC connection open while idle — the connection was already closed by the
            // try-with-resources above.
            if (gate != null) {
                Instant waitStart = Instant.now();
                LOGGER.info("Task {}, Window ended with active MoveIn gate for partition {} ({}), draining incrementally, buffered={}",
                        taskUid, token, gate.getSourcesByTimestamp(), gate.size());
                boolean interrupted = false;
                boolean timedOut = false;
                while (!gate.isEmpty()) {
                    List<ChangeStreamEvent> readySpinWait = gate.drainConfirmedPrefix();
                    if (!readySpinWait.isEmpty()) {
                        long elapsedMs = Duration.between(waitStart, Instant.now()).toMillis();
                        LOGGER.info("Task {}, MoveIn gate prefix flushed after {}ms for partition {} ({}), flushed={}, remaining={}",
                                taskUid, elapsedMs, token, gate.getSourcesByTimestamp(), readySpinWait.size(), gate.size());
                        for (ChangeStreamEvent e : readySpinWait) {
                            changeStreamEventConsumer.acceptChangeStreamEvent(e);
                        }
                    }
                    else {
                        long elapsedMs = Duration.between(waitStart, Instant.now()).toMillis();
                        if (elapsedMs >= moveInGateTimeoutMs) {
                            LOGGER.warn(
                                    "Task {}, MoveIn gate timed out after {}ms for partition {}, falling back to close/reopen path",
                                    taskUid, elapsedMs, partition.getToken());
                            timedOut = true;
                            break;
                        }
                        try {
                            Thread.sleep(moveInGateCheckIntervalMs);
                        }
                        catch (InterruptedException e) {
                            interrupted = true;
                            restoreInterruptAfterMoveIn = true;
                            break;
                        }
                    }
                }
                if (interrupted || timedOut) {
                    // Same reasoning as the result-set interrupt: do NOT transition to FINISHED
                    // while there are unflushed events in the buffer. Transition to MoveIn-pause
                    // (CREATED) instead so the partition is re-streamed from T1 on restart.
                    if (interrupted) {
                        LOGGER.info(
                                "Task {}, Spin-wait interrupted for partition {} with active gate — transitioning to MoveIn-pause to preserve unflushed events",
                                taskUid, partition.getToken());
                    }
                    if (moveInEvent == null) {
                        moveInEvent = gate.getFirstMoveInEvent();
                        moveInMetadata = gate.getFirstMoveInMetadata();
                    }
                    moveInAllSources = gate.getAllSources();
                    isPartitionMoveInEvent = true;
                    gate = null;
                    break;
                }
                long waitMs = Duration.between(waitStart, Instant.now()).toMillis();
                LOGGER.info("Task {}, MoveIn gate fully drained after {}ms for partition {}",
                        taskUid, waitMs, token);
                gate = null;
                gateIsFirst = true;
                // Immediately advance processedTimestamp in the sync topic so that a crash
                // immediately after this flush does not cause the buffered events to be
                // re-produced (duplicate records). The onWindowAdvanced() call at the bottom
                // of the outer-while body runs again with the same values — that is harmless
                // because BufferedPublisher coalesces the two produces within its 5ms window.
                if (newBoundaryRecordSequence != null) {
                    lastBoundaryRecordSequence = newBoundaryRecordSequence;
                }
                processedTimestamp = endTimestamp;
                partitionEventListener.onWindowAdvanced(partition, processedTimestamp, lastBoundaryRecordSequence);
            }

            if (partitionEndTimestamp != null && processedTimestamp.equals(partitionEndTimestamp)) {
                isPartitionEnded = true;
            }
            if (InitialPartition.isInitialPartition(token)) {
                isPartitionEnded = true;
            }

            if (newBoundaryRecordSequence != null) {
                lastBoundaryRecordSequence = newBoundaryRecordSequence;
            }
            processedTimestamp = endTimestamp;
            partitionEventListener.onWindowAdvanced(partition, processedTimestamp, lastBoundaryRecordSequence);
        }

        if (isPartitionMoveInEvent && moveInEvent != null) {
            // Use all accumulated sources when available (overflow / interrupt-with-gate paths),
            // so MoveInStateUpdateOperation waits for every source, not only the first MoveIn's.
            List<String> effectiveSources = (moveInAllSources != null && !moveInAllSources.isEmpty())
                    ? moveInAllSources
                    : moveInEvent.getSourcePartitions();
            LOGGER.info("Task {}, Pausing mutable partition {} after MoveIn event at {}, seq {}, sources {} (effectiveSources={})",
                    taskUid, partition, moveInEvent.getCommitTimestamp(), moveInEvent.getRecordSequence(),
                    moveInEvent.getSourcePartitions(), effectiveSources);
            if (moveInMetadata != null) {
                long commitMs = millis(moveInEvent.getCommitTimestamp());
                long queryStartedMs = millis(moveInMetadata.getQueryStartedAt());
                long streamStartedMs = millis(moveInMetadata.getRecordStreamStartedAt());
                long readAtMs = millis(moveInMetadata.getRecordReadAt());

                long commitToQueryMs = Math.max(0L, queryStartedMs - commitMs);
                long queryToStreamStartMs = Math.max(0L, streamStartedMs - Math.max(commitMs, queryStartedMs));
                long streamStartToReadMs = Math.max(0L, readAtMs - Math.max(commitMs, streamStartedMs));
                long commitToReadMs = Math.max(0L, readAtMs - commitMs);

                LOGGER.info(
                        "Task {}, MoveIn latency breakdown for partition {}: commitToQueryMs={} (staleness before we even asked), "
                                + "queryToStreamStartMs={} (RPC/query setup), streamStartToReadMs={} (waited inside open stream), "
                                + "commitToReadMs={} (total staleness at read time)",
                        taskUid, partition.getToken(),
                        commitToQueryMs, queryToStreamStartMs, streamStartToReadMs, commitToReadMs);

                metricsEventPublisher.publishMetricEvent(
                        new MoveInLatencyMetricEvent(commitToQueryMs, queryToStreamStartMs, streamStartToReadMs, commitToReadMs));
            }
            try {
                partitionEventListener.onMoveIn(partition, moveInEvent.getCommitTimestamp(), moveInEvent.getRecordSequence(), effectiveSources);
            }
            finally {
                if (restoreInterruptAfterMoveIn) {
                    Thread.currentThread().interrupt();
                }
            }
            return;
        }

        partitionEventListener.onFinish(partition);
        LOGGER.info("Task {}, Finished consuming mutable partition {}", taskUid, partition);

        changeStreamEventConsumer.acceptChangeStreamEvent(new FinishPartitionEvent(partition));
    }

    private List<ChangeStreamEvent> filterBoundaryDuplicates(
                                                             List<ChangeStreamEvent> events,
                                                             Timestamp windowStart,
                                                             String lastBoundaryRecordSequence) {
        if (lastBoundaryRecordSequence == null) {
            return events;
        }
        List<ChangeStreamEvent> filtered = new ArrayList<>();
        for (ChangeStreamEvent event : events) {
            if (isBeforeOrEqual(event.getRecordTimestamp(), windowStart)
                    && event.getRecordSequence() != null
                    && RecordSequenceUtils.compare(event.getRecordSequence(), lastBoundaryRecordSequence) <= 0) {
                LOGGER.debug("Task: {}, Skipping boundary duplicate event at {} seq {}",
                        taskUid, windowStart, event.getRecordSequence());
                continue;
            }
            filtered.add(event);
        }
        return filtered;
    }

    private long now() {
        return Instant.now().toEpochMilli();
    }

    private long millis(Timestamp timestamp) {
        return timestamp == null ? 0L : timestamp.toSqlTimestamp().toInstant().toEpochMilli();
    }

    private Timestamp addMinutes(Timestamp timestamp, Duration duration) {
        Instant result = Instant.ofEpochSecond(
                timestamp.getSeconds(),
                timestamp.getNanos()).plus(duration);

        return Timestamp.ofTimeSecondsAndNanos(result.getEpochSecond(), result.getNano());
    }

    private Timestamp minTimestamp(Timestamp a, Timestamp b) {
        int cmp = Long.compare(a.getSeconds(), b.getSeconds());
        if (cmp == 0) {
            cmp = Integer.compare(a.getNanos(), b.getNanos());
        }
        return cmp <= 0 ? a : b;
    }

    private boolean isBeforeOrEqual(Timestamp a, Timestamp b) {
        int cmp = Long.compare(a.getSeconds(), b.getSeconds());
        if (cmp == 0) {
            cmp = Integer.compare(a.getNanos(), b.getNanos());
        }
        return cmp <= 0;
    }

    private void processEvents(Partition partition, List<ChangeStreamEvent> events,
                               ChangeStreamEventConsumer changeStreamEventConsumer)
            throws InterruptedException {
        for (final ChangeStreamEvent changeStreamEvent : events) {
            if (changeStreamEvent instanceof ChildPartitionsEvent) {
                ChildPartitionsEvent childPartitionsEvent = (ChildPartitionsEvent) changeStreamEvent;
                LOGGER.info("Task: {}, Received child partition from partition {}:{}", taskUid, partition.getToken(), childPartitionsEvent);
            }
            LOGGER.debug("Task: {}, Received record from partition {}: {}", taskUid, partition.getToken(), changeStreamEvent);

            changeStreamEventConsumer.acceptChangeStreamEvent(changeStreamEvent);
        }
    }

}
