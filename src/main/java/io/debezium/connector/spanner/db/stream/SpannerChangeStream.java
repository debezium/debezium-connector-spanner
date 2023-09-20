/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.spanner.db.DatabaseClientFactory;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.stream.exception.ChangeStreamException;
import io.debezium.connector.spanner.db.stream.exception.FailureChangeStreamException;
import io.debezium.connector.spanner.db.stream.exception.OutOfRangeChangeStreamException;
import io.debezium.connector.spanner.db.stream.exception.StuckPartitionException;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.ActiveQueriesUpdateMetricEvent;
import io.debezium.connector.spanner.metrics.event.NewQueueMetricEvent;
import io.debezium.connector.spanner.metrics.event.RuntimeErrorMetricEvent;

/**
 * This class queries the change stream, andd sends the received records to ChangeStream Service.
 * Implementation of {@code ChangeStream}
 */
public class SpannerChangeStream implements ChangeStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerChangeStream.class);

    public static final Duration WAIT_TIMEOUT = Duration.ofMillis(200);

    private final SpannerChangeStreamService streamService;

    private final AtomicReference<ChangeStreamException> exception = new AtomicReference<>();

    private final PartitionThreadPool partitionThreadPool;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition signal = lock.newCondition();

    private final MetricsEventPublisher metricsEventPublisher;

    private volatile PartitionEventListener partitionEventListener;
    private volatile ChangeStreamEventConsumer changeStreamEventConsumer;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private final PartitionQueryingMonitor partitionQueryingMonitor;

    private final String taskUid;

    private final DatabaseClientFactory databaseClientFactory;

    public SpannerChangeStream(SpannerChangeStreamService streamService,
                               MetricsEventPublisher metricsEventPublisher, Duration heartBeatInterval, int maxMissedHeartbeats, String taskUid,
                               DatabaseClientFactory databaseClientFactory) {
        this.streamService = streamService;
        this.partitionThreadPool = new PartitionThreadPool();
        this.metricsEventPublisher = metricsEventPublisher;
        this.partitionQueryingMonitor = new PartitionQueryingMonitor(partitionThreadPool,
                heartBeatInterval,
                this::onStuckPartition,
                this::onError,
                metricsEventPublisher, maxMissedHeartbeats);
        this.taskUid = taskUid;
        this.databaseClientFactory = databaseClientFactory;
    }

    @Override
    public void run(BooleanSupplier runningFlagSupplier, ChangeStreamEventConsumer changeStreamEventConsumer,
                    PartitionEventListener partitionEventListener)
            throws ChangeStreamException, InterruptedException {

        this.changeStreamEventConsumer = changeStreamEventConsumer;

        this.partitionEventListener = partitionEventListener;

        this.isRunning.set(true);

        this.partitionQueryingMonitor.start();

        this.lock.lock();
        LOGGER.info("Task {}, Starting Spanner Change Stream", this.taskUid);

        try {
            while (runningFlagSupplier.getAsBoolean()) {
                if (signal.await(WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS) && exception.get() != null) {
                    LOGGER.warn("Task {}, is throwing exception during streaming {}", this.taskUid, exception.get());
                    throw exception.get();
                }

                metricsEventPublisher.publishMetricEvent(new ActiveQueriesUpdateMetricEvent(partitionThreadPool.getActiveThreads().size()));
            }

        }
        finally {
            this.partitionQueryingMonitor.stop();

            this.lock.unlock();

            LOGGER.info("Task {}, closing Spanner", this.taskUid);
            databaseClientFactory.closeSpanner();

            LOGGER.info("Task {}, Shutting down all partition streaming...", this.taskUid);
            this.isRunning.set(false);
            this.partitionThreadPool.shutdown(this.taskUid);
            LOGGER.info("Task {}, Shutdown all partition streaming...", this.taskUid);
        }
    }

    @Override
    public boolean submitPartition(Partition partition) {
        if (!isRunning.get()) {
            LOGGER.warn("Task {}, Failed to submit partition: {}", this.taskUid, partition.getToken());
            return false;
        }

        boolean submitted = partitionThreadPool.submit(partition.getToken(), () -> {
            LOGGER.info("task {}, Started streaming from partition with token {}", this.taskUid, partition.getToken());
            try {
                streamService.getEvents(partition, this::onStreamEvent, this.partitionEventListener);
            }
            catch (InterruptedException ex) {
                LOGGER.info("task {}, Interrupting streaming partition task with token {}", this.taskUid, partition.getToken());
                Thread.currentThread().interrupt();
            }
            catch (Exception ex) {

                LOGGER.info("Task {}, Exception during streaming {} from partition with token {}", this.taskUid, ex.getMessage(), partition.getToken());

                if (this.onError(partition, ex)) {
                    LOGGER.info("Task {}, Unretriable exception during streaming {} from partition with token {}", this.taskUid, ex.getMessage(), partition.getToken());
                    return;
                }

                try {
                    partitionEventListener.onException(partition, ex);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.info("Interrupting streaming partition task with token {} and exception {}, SHOULD NEVER REACH THIS POINT, CHECK IF TASK FAILED",
                            partition.getToken(), e);
                }
            }
            finally {
                LOGGER.info("Task {}, Stopped streaming from partition with token {}", this.taskUid, partition.getToken());
            }
        });

        if (submitted) {
            metricsEventPublisher.publishMetricEvent(new NewQueueMetricEvent());
            metricsEventPublisher.publishMetricEvent(new ActiveQueriesUpdateMetricEvent(partitionThreadPool.getActiveThreads().size()));
        }

        return submitted;
    }

    @VisibleForTesting
    void onStreamEvent(ChangeStreamEvent changeStreamEvent) throws InterruptedException {
        this.partitionQueryingMonitor.acceptStreamEvent(changeStreamEvent);
        this.changeStreamEventConsumer.acceptChangeStreamEvent(changeStreamEvent);
    }

    @VisibleForTesting
    void onStuckPartition(String token) throws InterruptedException {
        LOGGER.warn("Partition {} is stuck", token);
        this.partitionThreadPool.stop(token);
        if (this.partitionEventListener.onStuckPartition(token)) {
            this.onError(new StuckPartitionException(token));
        }
    }

    @Override
    public void stop() {
        LOGGER.info("Task {}, closing Spanner", this.taskUid);
        databaseClientFactory.closeSpanner();

        LOGGER.info("Task {}, Shutting down partition thread pool {}", this.taskUid, partitionThreadPool.getActiveThreads());

        partitionThreadPool.shutdown(this.taskUid);
        LOGGER.info("Task {}, Shutdown partition thread pool", this.taskUid);
    }

    @VisibleForTesting
    boolean onError(Partition partition, Exception ex) {
        ChangeStreamException changeStreamException = getStreamException(partition, ex);

        return onError(changeStreamException);
    }

    @VisibleForTesting
    boolean onError(ChangeStreamException ex) {
        if (ex instanceof FailureChangeStreamException) {
            exception.compareAndSet(null, ex);
            signal();
            return true;
        }
        metricsEventPublisher.publishMetricEvent(new RuntimeErrorMetricEvent());

        return false;
    }

    @VisibleForTesting
    boolean isCanceled(Exception ex) {
        if (ex instanceof SpannerException) {
            SpannerException spannerException = (SpannerException) ex;

            if (spannerException.getErrorCode().equals(ErrorCode.CANCELLED)) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    ChangeStreamException getStreamException(Partition partition, Exception ex) {
        if (ex instanceof SpannerException) {
            SpannerException spannerException = (SpannerException) ex;

            switch (spannerException.getErrorCode()) {
                case OUT_OF_RANGE:
                    return new OutOfRangeChangeStreamException(partition, spannerException);
                case INVALID_ARGUMENT:
                default:
                    return new ChangeStreamException(spannerException);
            }
        }

        return new FailureChangeStreamException(ex);
    }

    private void signal() {
        lock.lock();
        try {
            signal.signal();
        }
        finally {
            lock.unlock();
        }
    }

}
