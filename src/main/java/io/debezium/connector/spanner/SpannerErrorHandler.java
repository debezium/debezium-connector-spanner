/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Handles all types during the Connector runtime
 * and propagates them to ChangeEventQueue
 */
public class SpannerErrorHandler extends ErrorHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerErrorHandler.class);
    private final AtomicReference<Throwable> producerThrowable = new AtomicReference<>(null);

    private final ChangeEventQueue<?> queue;
    private final SpannerConnectorTask task;

    public SpannerErrorHandler(SpannerConnectorTask task, ChangeEventQueue<?> queue) {
        super(SpannerConnector.class, null, queue);
        this.task = task;
        this.queue = queue;
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        return true;
    }

    @Override
    public void setProducerThrowable(Throwable producerThrowable) {
        boolean first = this.producerThrowable.compareAndSet(null, producerThrowable);

        if (first) {
            LOGGER.error("Task failure, taskUid: {}, {}", task.getTaskUid(), getStackTrace(producerThrowable));
        }
        else {
            LOGGER.warn("Follow-up failure exception, taskUid: {}, {}", task.getTaskUid(), getStackTrace(producerThrowable));
        }

        boolean retriable = isRetriable(producerThrowable);

        LOGGER.info("Checking up on Task failure for task {}", task.getTaskUid());

        if (first) {
            if (retriable) {
                LOGGER.info("Queueing retriable exception {} for task {}", producerThrowable, task.getTaskUid());
                queue.producerException(
                        new RetriableException("An exception occurred in the change event producer. This connector will be restarted.", producerThrowable));
            }
            else {
                LOGGER.info("Queueing unretriable exception {} for task {}", producerThrowable, task.getTaskUid());
                queue.producerException(new ConnectException("An exception occurred in the change event producer. This connector will be stopped.", producerThrowable));
            }
        }
    }

    @Override
    public Throwable getProducerThrowable() {
        return producerThrowable.get();
    }

    public static String getStackTrace(Throwable ex) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);

        return sw.toString();
    }
}
