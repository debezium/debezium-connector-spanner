/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerConnectorTask;
import io.debezium.connector.spanner.SpannerErrorHandler;
import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.function.BlockingSupplier;
import io.debezium.connector.spanner.metrics.event.ActiveQueriesUpdateMetricEvent;
import io.debezium.connector.spanner.metrics.event.ChildPartitionsMetricEvent;
import io.debezium.connector.spanner.metrics.event.DelayChangeStreamEventsMetricEvent;
import io.debezium.connector.spanner.metrics.event.LatencyMetricEvent;
import io.debezium.connector.spanner.metrics.event.NewQueueMetricEvent;
import io.debezium.connector.spanner.metrics.event.OffsetReceivingTimeMetricEvent;
import io.debezium.connector.spanner.metrics.event.PartitionOffsetLagMetricEvent;
import io.debezium.connector.spanner.metrics.event.RebalanceMetricEvent;
import io.debezium.connector.spanner.metrics.event.RuntimeErrorMetricEvent;
import io.debezium.connector.spanner.metrics.event.SpannerEventQueueUpdateEvent;
import io.debezium.connector.spanner.metrics.event.StuckHeartbeatIntervalsMetricEvent;
import io.debezium.connector.spanner.metrics.event.TaskStateChangeQueueUpdateMetricEvent;
import io.debezium.connector.spanner.metrics.event.TaskSyncContextMetricEvent;
import io.debezium.connector.spanner.metrics.latency.LatencyCalculator;
import io.debezium.connector.spanner.metrics.latency.Statistics;
import io.debezium.connector.spanner.task.TaskSyncContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Collects metrics of the Spanner connector
 */
public class SpannerMeter {

    private final Set<DataCollectionId> capturedTables = ConcurrentHashMap.newKeySet();
    private final AtomicInteger detectedPartitionCount = new AtomicInteger(0);
    private final AtomicInteger numberOfQueriesIssuedCount = new AtomicInteger(0);
    private final AtomicInteger numberOfActiveQueries = new AtomicInteger(0);
    private final AtomicInteger stuckHeartbeatIntervals = new AtomicInteger(0);

    private final AtomicInteger errorCount = new AtomicInteger(0);

    private final AtomicInteger spannerEventQueueTotalCapacity = new AtomicInteger(0);

    private final AtomicInteger spannerEventQueueRemainingCapacity = new AtomicInteger(0);

    private final AtomicInteger taskStateChangeEventQueueRemainingCapacity = new AtomicInteger(0);

    private final MetricsEventPublisher metricsEventPublisher;

    private final BlockingSupplier<Timestamp> lowWatermarkSupplier;

    private volatile TaskSyncContext taskSyncContext;

    private final SpannerConnectorTask spannerConnectorTask;

    private final AtomicInteger rebalanceAnswersActual = new AtomicInteger();
    private final AtomicInteger rebalanceAnswersExpected = new AtomicInteger();

    private final Statistics totalLatency;
    private final Statistics connectorLatency;
    private final Statistics spannerLatency;
    private final Statistics commitToEmitLatency;

    private final Statistics commitToPublishLatency;

    private final Statistics emitToPublishLatency;

    private final Statistics ownConnectorLatency;

    private final Statistics lowWatermarkLagLatency;

    private final Statistics partitionOffsetLagStatistics;

    private final Statistics receivingTimeOffsetStatistics;

    private final Statistics delayChangeStreamEvents;

    private final SpannerConnectorConfig connectorConfig;

    private final SpannerErrorHandler spannerErrorHandler;

    public SpannerMeter(SpannerConnectorTask task, SpannerConnectorConfig connectorConfig,
                        SpannerErrorHandler errorHandler,
                        BlockingSupplier<Timestamp> lowWatermarkSupplier) {
        this.metricsEventPublisher = new MetricsEventPublisher();

        this.spannerConnectorTask = task;
        this.connectorConfig = connectorConfig;

        this.spannerErrorHandler = errorHandler;

        this.lowWatermarkSupplier = lowWatermarkSupplier;

        this.totalLatency = new Statistics(connectorConfig.percentageMetricsClearInterval(), this::onError);
        this.connectorLatency = new Statistics(connectorConfig.percentageMetricsClearInterval(), this::onError);
        this.spannerLatency = new Statistics(connectorConfig.percentageMetricsClearInterval(), this::onError);
        this.commitToEmitLatency = new Statistics(connectorConfig.percentageMetricsClearInterval(), this::onError);
        this.commitToPublishLatency = new Statistics(connectorConfig.percentageMetricsClearInterval(), this::onError);
        this.emitToPublishLatency = new Statistics(connectorConfig.percentageMetricsClearInterval(), this::onError);
        this.lowWatermarkLagLatency = new Statistics(connectorConfig.percentageMetricsClearInterval(), this::onError);
        this.ownConnectorLatency = new Statistics(connectorConfig.percentageMetricsClearInterval(), this::onError);
        this.partitionOffsetLagStatistics = new Statistics(connectorConfig.percentageMetricsClearInterval(), this::onError);
        this.receivingTimeOffsetStatistics = new Statistics(connectorConfig.percentageMetricsClearInterval(), this::onError);
        this.delayChangeStreamEvents = new Statistics(connectorConfig.percentageMetricsClearInterval(), this::onError);

        metricsEventPublisher.subscribe(ChildPartitionsMetricEvent.class,
                event -> detectedPartitionCount.addAndGet(event.getNumberPartitions()));

        metricsEventPublisher.subscribe(NewQueueMetricEvent.class,
                event -> numberOfQueriesIssuedCount.incrementAndGet());

        metricsEventPublisher.subscribe(ActiveQueriesUpdateMetricEvent.class,
                event -> numberOfActiveQueries.set(event.getActiveQueries()));

        metricsEventPublisher.subscribe(StuckHeartbeatIntervalsMetricEvent.class,
                event -> stuckHeartbeatIntervals.set(event.getStuckHeartbeatIntervals()));

        metricsEventPublisher.subscribe(RuntimeErrorMetricEvent.class,
                event -> errorCount.incrementAndGet());

        metricsEventPublisher.subscribe(TaskSyncContextMetricEvent.class, event -> taskSyncContext = event.getTaskSyncContext());

        metricsEventPublisher.subscribe(LatencyMetricEvent.class, event -> {

            if (event.getTotalLatency() != null) {
                totalLatency.update(event.getTotalLatency());
            }
            if (event.getReadToEmitLatency() != null) {
                connectorLatency.update(event.getReadToEmitLatency());
            }
            if (event.getSpannerLatency() != null) {
                spannerLatency.update(event.getSpannerLatency());
            }
            if (event.getCommitToEmitLatency() != null) {
                commitToEmitLatency.update(event.getCommitToEmitLatency());
            }
            if (event.getCommitToPublishLatency() != null) {
                commitToPublishLatency.update(event.getCommitToPublishLatency());
            }
            if (event.getEmitToPublishLatency() != null) {
                emitToPublishLatency.update(event.getEmitToPublishLatency());
            }
            if (event.getLowWatermarkLag() != null) {
                lowWatermarkLagLatency.update(event.getLowWatermarkLag());
            }
            if (event.getOwnConnectorLatency() != null) {
                ownConnectorLatency.update(event.getOwnConnectorLatency());
            }
        });

        metricsEventPublisher.subscribe(SpannerEventQueueUpdateEvent.class, event -> {
            spannerEventQueueTotalCapacity.set(event.getTotalCapacity());

            spannerEventQueueRemainingCapacity.set(event.getRemainingCapacity());
        });

        metricsEventPublisher.subscribe(PartitionOffsetLagMetricEvent.class, event -> {
            if (InitialPartition.isInitialPartition(event.getToken())) {
                return;
            }
            partitionOffsetLagStatistics.update(event.getOffsetLag());
        });

        metricsEventPublisher.subscribe(RebalanceMetricEvent.class, event -> {
            rebalanceAnswersActual.set(event.getRebalanceAnswersActual());
            rebalanceAnswersExpected.set(event.getRebalanceAnswersExpected());
        });

        metricsEventPublisher.subscribe(OffsetReceivingTimeMetricEvent.class, event -> {
            receivingTimeOffsetStatistics.update(event.getTime());
        });

        metricsEventPublisher.subscribe(DelayChangeStreamEventsMetricEvent.class,
                event -> delayChangeStreamEvents.update(event.getDelayChangeStreamEvents()));

        metricsEventPublisher.subscribe(TaskStateChangeQueueUpdateMetricEvent.class, event -> {
            taskStateChangeEventQueueRemainingCapacity.set(event.getRemainingCapacity());
        });
    }

    private void onError(Throwable throwable) {
        this.spannerErrorHandler.setProducerThrowable(throwable);
    }

    public MetricsEventPublisher getMetricsEventPublisher() {
        return metricsEventPublisher;
    }

    public void captureTable(DataCollectionId dataCollectionId) {
        this.capturedTables.add(dataCollectionId);
    }

    public Set<DataCollectionId> getCapturedTables() {
        return capturedTables;
    }

    public void reset() {
        capturedTables.clear();

        totalLatency.reset();
        connectorLatency.reset();
        spannerLatency.reset();
        commitToEmitLatency.reset();

        commitToPublishLatency.reset();
        emitToPublishLatency.reset();

        ownConnectorLatency.reset();

        partitionOffsetLagStatistics.reset();

        lowWatermarkLagLatency.reset();

        receivingTimeOffsetStatistics.reset();

        delayChangeStreamEvents.reset();
    }

    public void start() {
        totalLatency.start();
        connectorLatency.start();
        spannerLatency.start();
        commitToEmitLatency.start();

        commitToPublishLatency.start();
        emitToPublishLatency.start();

        ownConnectorLatency.start();

        partitionOffsetLagStatistics.start();

        lowWatermarkLagLatency.start();

        receivingTimeOffsetStatistics.start();

        delayChangeStreamEvents.start();
    }

    public void shutdown() {
        totalLatency.shutdown();
        connectorLatency.shutdown();
        spannerLatency.shutdown();
        commitToEmitLatency.shutdown();

        commitToPublishLatency.shutdown();
        emitToPublishLatency.shutdown();

        ownConnectorLatency.shutdown();

        partitionOffsetLagStatistics.shutdown();

        lowWatermarkLagLatency.shutdown();

        receivingTimeOffsetStatistics.shutdown();

        delayChangeStreamEvents.shutdown();
    }

    public void finishTask() {
        this.spannerConnectorTask.finish();
    }

    public void restartTask() {
        this.spannerConnectorTask.restart();
    }

    public TaskSyncContext getTaskSyncContext() {
        return taskSyncContext;
    }

    public String getTaskUid() {
        return spannerConnectorTask.getTaskUid();
    }

    public Statistics getTotalLatency() {
        return totalLatency;
    }

    public Statistics getConnectorLatency() {
        return connectorLatency;
    }

    public Statistics getSpannerLatency() {
        return spannerLatency;
    }

    public Statistics getCommitToEmitLatency() {
        return commitToEmitLatency;
    }

    public Statistics getCommitToPublishLatency() {
        return commitToPublishLatency;
    }

    public Statistics getEmitToPublishLatency() {
        return emitToPublishLatency;
    }

    public Statistics getLowWatermarkLagLatency() {
        return lowWatermarkLagLatency;
    }

    public Statistics getOwnConnectorLatency() {
        return ownConnectorLatency;
    }

    public Statistics getPartitionOffsetLagStatistics() {
        return partitionOffsetLagStatistics;
    }

    public Statistics getOffsetReceivingTimeStatistics() {
        return receivingTimeOffsetStatistics;
    }

    public Long getLowWatermarkLag() throws InterruptedException {
        if (!connectorConfig.isLowWatermarkEnabled()) {
            return null;
        }
        return LatencyCalculator.getTimeBehindLowWatermark(lowWatermarkSupplier.get());
    }

    public Timestamp getLowWatermark() throws InterruptedException {
        if (!connectorConfig.isLowWatermarkEnabled()) {
            return null;
        }
        return lowWatermarkSupplier.get();
    }

    public int getNumberOfPartitionsDetected() {
        return detectedPartitionCount.get();
    }

    public int getNumberOfQueriesIssuedTotal() {
        return numberOfQueriesIssuedCount.get();
    }

    public int getNumberOfActiveQueries() {
        return numberOfActiveQueries.get();
    }

    public int getStuckHeartbeatIntervals() {
        return stuckHeartbeatIntervals.get();
    }

    public Statistics getDelayChangeStreamEvents() {
        return delayChangeStreamEvents;
    }

    public int getErrorCount() {
        return errorCount.get();
    }

    public int getSpannerEventQueueTotalCapacity() {
        return spannerEventQueueTotalCapacity.get();
    }

    public int getSpannerEventQueueRemainingCapacity() {
        return spannerEventQueueRemainingCapacity.get();
    }

    public int getTaskStateChangeEventQueueRemainingCapacity() {
        return taskStateChangeEventQueueRemainingCapacity.get();
    }

    public long getRebalanceGenerationId() {
        return taskSyncContext.getRebalanceGenerationId();
    }

    public int getRebalanceAnswersActual() {
        return rebalanceAnswersActual.get();
    }

    public int getRebalanceAnswersExpected() {
        return rebalanceAnswersExpected.get();
    }

    public boolean isLeader() {
        return this.taskSyncContext.isLeader();
    }
}
