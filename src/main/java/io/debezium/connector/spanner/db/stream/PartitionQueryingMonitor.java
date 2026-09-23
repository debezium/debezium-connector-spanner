/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.PartitionKey;
import io.debezium.connector.spanner.db.model.StreamEventMetadata;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.stream.exception.ChangeStreamException;
import io.debezium.connector.spanner.db.stream.exception.FailureChangeStreamException;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.StuckHeartbeatIntervalsMetricEvent;
import io.debezium.function.BlockingConsumer;

/**
 * Monitors partition querying. If maxMissedEvents is reached, onStuckPartitionConsumer is called.
 */
public class PartitionQueryingMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionThreadPool.class);
    private static final Duration CHECK_INTERVAL = Duration.of(60000, ChronoUnit.MILLIS);

    private final PartitionThreadPool partitionThreadPool;
    private final long heartBeatIntervalMillis;
    private final Duration timeout;
    private volatile Thread thread;
    private final Map<PartitionKey, Instant> lastEventTimestampMap = new ConcurrentHashMap<>();

    private final Consumer<ChangeStreamException> errorConsumer;

    private final StuckPartitionConsumer onStuckPartitionConsumer;

    private final MetricsEventPublisher metricsEventPublisher;

    public PartitionQueryingMonitor(
                                    PartitionThreadPool partitionThreadPool,
                                    Duration heartBeatInterval,
                                    BlockingConsumer<String> onStuckPartitionConsumer,
                                    Consumer<ChangeStreamException> errorConsumer,
                                    MetricsEventPublisher metricsEventPublisher,
                                    int maxMissedEvents) {
        this(partitionThreadPool, heartBeatInterval, (token, tvfName) -> onStuckPartitionConsumer.accept(token),
                errorConsumer, metricsEventPublisher, maxMissedEvents);
    }

    public PartitionQueryingMonitor(
                                    PartitionThreadPool partitionThreadPool,
                                    Duration heartBeatInterval,
                                    StuckPartitionConsumer onStuckPartitionConsumer,
                                    Consumer<ChangeStreamException> errorConsumer,
                                    MetricsEventPublisher metricsEventPublisher,
                                    int maxMissedEvents) {
        this.partitionThreadPool = partitionThreadPool;
        this.heartBeatIntervalMillis = heartBeatInterval.toMillis();

        this.timeout = Duration.of(heartBeatInterval.toMillis() * maxMissedEvents, ChronoUnit.MILLIS);

        this.errorConsumer = errorConsumer;

        this.onStuckPartitionConsumer = onStuckPartitionConsumer;

        this.metricsEventPublisher = metricsEventPublisher;
    }

    public void checkPartitionThreads() throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            Set<PartitionKey> activePartitions = partitionThreadPool.getActivePartitions();

            Set<PartitionKey> toRemove = lastEventTimestampMap.keySet().stream()
                    .filter(partition -> !activePartitions.contains(partition))
                    .collect(Collectors.toSet());

            toRemove.forEach(lastEventTimestampMap::remove);

            int maxStuckHeartbeatIntervals = -1;
            for (PartitionKey partition : activePartitions) {
                // Only measure stuck heartbeat interval for the partition queries.
                if (InitialPartition.isInitialPartition(partition.getToken())) {
                    continue;
                }

                Instant lastEventTimestamp = lastEventTimestampMap.get(partition);
                if (lastEventTimestamp == null) {
                    lastEventTimestampMap.put(partition, Instant.now());
                    continue;
                }
                LOGGER.info("PartitionQueryingMonitor, partition {} last received timestamp {}", partition, lastEventTimestamp);

                int stuckHeartbeatIntervals = stuckHeartbeatIntervals(lastEventTimestamp);
                if (stuckHeartbeatIntervals > maxStuckHeartbeatIntervals) {
                    metricsEventPublisher.publishMetricEvent(
                            new StuckHeartbeatIntervalsMetricEvent(stuckHeartbeatIntervals));
                    maxStuckHeartbeatIntervals = stuckHeartbeatIntervals;
                }

                if (isPartitionStuck(lastEventTimestamp)) {
                    lastEventTimestampMap.remove(partition);
                    onStuckPartitionConsumer.accept(partition.getToken(), partition.getTvfName());
                }
            }

            Thread.sleep(CHECK_INTERVAL.toMillis());
        }
    }

    @VisibleForTesting
    int stuckHeartbeatIntervals(Instant lastEventInstant) {
        long stuckMillis = Duration.between(lastEventInstant, Instant.now()).toMillis();
        long stuckIntervals = stuckMillis / heartBeatIntervalMillis;
        return ((int) stuckIntervals);
    }

    @VisibleForTesting
    boolean isPartitionStuck(Instant lastEventInstant) {
        return lastEventInstant.isBefore(Instant.now().minus(timeout));
    }

    public void start() {
        if (this.thread != null) {
            return;
        }
        this.thread = new Thread(
                () -> {
                    try {
                        checkPartitionThreads();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                "SpannerConnector-PartitionQueryingMonitor");

        this.thread.setUncaughtExceptionHandler(
                (t, ex) -> {
                    this.errorConsumer.accept(
                            new FailureChangeStreamException(
                                    "PartitionQueryingMonitor error", new RuntimeException(ex)));
                });

        this.thread.start();
    }

    public void stop() {
        if (this.thread == null) {
            return;
        }
        this.thread.interrupt();
    }

    public void acceptStreamEvent(ChangeStreamEvent changeStreamEvent) {
        StreamEventMetadata metadata = changeStreamEvent.getMetadata();
        this.lastEventTimestampMap.put(
                new PartitionKey(metadata.getPartitionToken(), metadata.getTvfName()), Instant.now());
    }

    @VisibleForTesting
    boolean hasReceivedEvent(String token, String tvfName) {
        return lastEventTimestampMap.containsKey(new PartitionKey(token, tvfName));
    }

    @FunctionalInterface
    public interface StuckPartitionConsumer {
        void accept(String token, String tvfName) throws InterruptedException;
    }
}
