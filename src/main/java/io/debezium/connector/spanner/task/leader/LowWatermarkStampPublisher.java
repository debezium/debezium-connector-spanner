/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.leader;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.connector.spanner.task.TaskSyncContextHolder;

/**
 * Generates watermark update messages to output topics with the latest
 * watermark value
 */
public class LowWatermarkStampPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(LowWatermarkStampPublisher.class);

    private final Duration publishInterval;

    private volatile Thread publisherThread;

    private final SpannerEventDispatcher spannerEventDispatcher;
    private final boolean lowWatermarkEnabled;

    private final AtomicBoolean suspendFlag = new AtomicBoolean(false);

    private final Consumer<Throwable> errorHandler;

    private final TaskSyncContextHolder taskSyncContextHolder;

    public LowWatermarkStampPublisher(SpannerConnectorConfig spannerConnectorConfig,
                                      SpannerEventDispatcher spannerEventDispatcher,
                                      Consumer<Throwable> errorHandler,
                                      TaskSyncContextHolder taskSyncContextHolder) {
        this.publishInterval = spannerConnectorConfig.getLowWatermarkStampInterval();
        this.spannerEventDispatcher = spannerEventDispatcher;
        this.lowWatermarkEnabled = spannerConnectorConfig.isLowWatermarkEnabled();
        this.errorHandler = errorHandler;
        this.taskSyncContextHolder = taskSyncContextHolder;
    }

    public void init() {
        if (!lowWatermarkEnabled || this.publisherThread != null) {
            return;
        }
        this.publisherThread = createPublisherThread();
    }

    public void start() {
        if (!lowWatermarkEnabled) {
            return;
        }

        if (publisherThread.getState().equals(Thread.State.NEW)) {
            this.publisherThread.start();
        }

        this.suspendFlag.compareAndSet(true, false);
    }

    public void suspend() {
        this.suspendFlag.set(true);
    }

    public void destroy() throws InterruptedException {
        if (this.publisherThread == null || publisherThread.getState().equals(Thread.State.NEW)) {
            return;
        }
        this.suspendFlag.set(true);

        this.publisherThread.interrupt();

        while (this.publisherThread != null) {
        }

        spannerEventDispatcher.publishLowWatermarkStampEvent();
    }

    private Thread createPublisherThread() {
        Thread thread = new Thread(() -> {
            try {
                while (!taskSyncContextHolder.get().isInitialized() && !Thread.currentThread().isInterrupted()) {
                }

                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        if (!suspendFlag.get()) {
                            spannerEventDispatcher.publishLowWatermarkStampEvent();
                        }

                        Thread.sleep(publishInterval.toMillis());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            finally {
                this.publisherThread = null;
            }
        }, "SpannerConnector-LowWatermarkStampPublisher");

        thread.setUncaughtExceptionHandler((t, ex) -> {
            LOGGER.error("LowWatermarkStampPublisher execution error", ex);
            this.publisherThread = null;

            this.errorHandler.accept(ex);
        });

        return thread;
    }

}
