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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.dao.ChangeStreamDao;
import io.debezium.connector.spanner.db.dao.ChangeStreamResultSet;
import io.debezium.connector.spanner.db.mapper.ChangeStreamRecordMapper;
import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.model.event.ChildPartitionsEvent;
import io.debezium.connector.spanner.db.model.event.FinishPartitionEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEndEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEventEvent;
import io.debezium.connector.spanner.db.model.event.RecordSequenceUtils;
import io.debezium.connector.spanner.db.stream.exception.ChangeStreamException;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.DelayChangeStreamEventsMetricEvent;

/**
 * This class queries the change stream, sends child partitions to SynchronizedPartitionManager,
 * and updates the last commit timestamp for each partition.
 */
public class SpannerChangeStreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerChangeStreamService.class);

    private final ChangeStreamDao changeStreamDao;
    private final ChangeStreamRecordMapper changeStreamRecordMapper;

    private final Duration heartbeatMillis;
    private final MetricsEventPublisher metricsEventPublisher;
    private final String taskUid;
    private final Duration windowDuration;
    private final boolean mutablePartitionOrderingEnabled;

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher) {
        this(taskUid, changeStreamDao, changeStreamRecordMapper, heartbeatMillis, metricsEventPublisher, 20);
    }

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher, int windowMinutes) {
        this(taskUid, changeStreamDao, changeStreamRecordMapper, heartbeatMillis, metricsEventPublisher, windowMinutes, true);
    }

    public SpannerChangeStreamService(String taskUid, ChangeStreamDao changeStreamDao, ChangeStreamRecordMapper changeStreamRecordMapper,
                                      Duration heartbeatMillis, MetricsEventPublisher metricsEventPublisher, int windowMinutes,
                                      boolean mutablePartitionOrderingEnabled) {
        this.changeStreamDao = changeStreamDao;
        this.changeStreamRecordMapper = changeStreamRecordMapper;
        this.heartbeatMillis = heartbeatMillis;
        this.metricsEventPublisher = metricsEventPublisher;
        this.taskUid = taskUid;
        this.windowDuration = Duration.ofMinutes(windowMinutes);
        this.mutablePartitionOrderingEnabled = mutablePartitionOrderingEnabled;
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
                    if (heartbeatLag > 60_000) {
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
        boolean isPartitionMoveInEvent = false;
        PartitionEventEvent moveInEvent = null;

        while (!isPartitionEnded && !isPartitionMoveInEvent
                && (partitionEndTimestamp == null || isBeforeOrEqual(processedTimestamp, partitionEndTimestamp))) {
            Timestamp endTimestamp = partitionEndTimestamp == null
                    ? addMinutes(processedTimestamp, windowDuration)
                    : minTimestamp(partitionEndTimestamp, addMinutes(processedTimestamp, windowDuration));
            String newBoundaryRecordSequence = null;

            try (ChangeStreamResultSet resultSet = changeStreamDao.streamQuery(token, processedTimestamp,
                    endTimestamp, heartbeatMillis.toMillis())) {

                long start = now();
                while (resultSet.next()) {
                    long delay = now() - start;

                    List<ChangeStreamEvent> rawEvents = changeStreamRecordMapper.toChangeStreamEvents(
                            partition,
                            resultSet, resultSet.getMetadata());
                    LOGGER.debug("Task: {}, Events receive from mutable stream: {}", taskUid, rawEvents);

                    List<ChangeStreamEvent> events = filterBoundaryDuplicates(rawEvents, processedTimestamp, lastBoundaryRecordSequence);

                    if (!events.isEmpty() && (events.get(0) instanceof HeartbeatEvent)) {
                        var heartbeatEvent = (HeartbeatEvent) events.get(0);
                        long heartbeatLag = System.currentTimeMillis() - heartbeatEvent.getRecordTimestamp().toSqlTimestamp().toInstant().toEpochMilli();
                        if (heartbeatLag > 60_000) {
                            LOGGER.warn("Task: {}, heartbeat has very old timestamp, lag: {}, token: {}, event: {}", taskUid, heartbeatLag,
                                    heartbeatEvent.getMetadata().getPartitionToken(),
                                    heartbeatEvent);
                        }
                    }

                    for (ChangeStreamEvent event : events) {
                        if (endTimestamp.equals(event.getRecordTimestamp()) && event.getRecordSequence() != null) {
                            newBoundaryRecordSequence = event.getRecordSequence();
                        }
                    }

                    processEvents(partition, events, changeStreamEventConsumer);

                    for (ChangeStreamEvent event : events) {
                        if (event instanceof PartitionEndEvent) {
                            isPartitionEnded = true;
                        }
                        if (event instanceof PartitionEventEvent && mutablePartitionOrderingEnabled) {
                            PartitionEventEvent partitionEventEvent = (PartitionEventEvent) event;
                            if (!partitionEventEvent.getSourcePartitions().isEmpty()) {
                                isPartitionMoveInEvent = true;
                                moveInEvent = partitionEventEvent;
                            }
                        }
                    }

                    if (!events.isEmpty() && !(events.get(0) instanceof HeartbeatEvent)) {
                        metricsEventPublisher.publishMetricEvent(new DelayChangeStreamEventsMetricEvent((int) delay));
                    }

                    if (isPartitionMoveInEvent) {
                        break;
                    }

                    start = now();
                }
            }
            catch (InterruptedException ex) {
                LOGGER.info("task {}, Interrupting streaming mutable partition task with token {}", this.taskUid, partition.getToken());
                Thread.currentThread().interrupt();
                break;
            }

            if (isPartitionMoveInEvent) {
                break;
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
            LOGGER.info("Task {}, Pausing mutable partition {} after MoveIn event at {}, seq {}, sources {}",
                    taskUid, partition, moveInEvent.getCommitTimestamp(), moveInEvent.getRecordSequence(), moveInEvent.getSourcePartitions());
            partitionEventListener.onMoveIn(partition, moveInEvent.getCommitTimestamp(), moveInEvent.getRecordSequence(), moveInEvent.getSourcePartitions());
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
