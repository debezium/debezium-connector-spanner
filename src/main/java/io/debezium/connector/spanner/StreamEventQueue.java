/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.SpannerEventQueueUpdateEvent;

/**
 * Internal queue which holds Spanner Events
 * before they will be processed
 */
public class StreamEventQueue {
    private final BlockingDeque<ChangeStreamEvent> queue;
    private final MetricsEventPublisher metricsEventPublisher;

    public StreamEventQueue(int capacity, MetricsEventPublisher metricsEventPublisher) {
        this.queue = new LinkedBlockingDeque<>(capacity);
        this.metricsEventPublisher = metricsEventPublisher;

        this.metricsEventPublisher.publishMetricEvent(
                new SpannerEventQueueUpdateEvent(queue.remainingCapacity() + queue.size(), queue.remainingCapacity()));
    }

    public void put(ChangeStreamEvent changeStreamEvent) throws InterruptedException {
        this.queue.put(changeStreamEvent);

        this.metricsEventPublisher.publishMetricEvent(
                new SpannerEventQueueUpdateEvent(queue.remainingCapacity() + queue.size(), queue.remainingCapacity()));
    }

    public ChangeStreamEvent take() throws InterruptedException {
        ChangeStreamEvent event = this.queue.take();

        this.metricsEventPublisher.publishMetricEvent(
                new SpannerEventQueueUpdateEvent(queue.remainingCapacity() + queue.size(), queue.remainingCapacity()));

        return event;
    }

}
