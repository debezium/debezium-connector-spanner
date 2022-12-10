/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * This class allows to publish the latest buffered value
 * once per time period, except the case: if the value is required
 * to be published immediately.
 */
public class BufferedPublisher<V> {

    private final Thread thread;
    private final AtomicReference<V> value = new AtomicReference<>();
    private final Predicate<V> publishImmediately;
    private final Consumer<V> onPublish;

    public BufferedPublisher(String name, long timeout, Predicate<V> publishImmediately, Consumer<V> onPublish) {
        this.publishImmediately = publishImmediately;
        this.onPublish = onPublish;

        this.thread = new Thread(() -> {

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    publishBuffered();
                    Thread.sleep(timeout);
                }
                catch (InterruptedException e) {
                    return;
                }
            }
        }, "SpannerConnector-" + name);
    }

    public void buffer(V update) {
        if (publishImmediately.test(update)) {
            synchronized (this) {
                this.value.set(null);
                this.onPublish.accept(update);
            }
        }
        else {
            value.set(update);
        }
    }

    private synchronized void publishBuffered() {
        V item = this.value.getAndSet(null);

        if (item != null) {
            this.onPublish.accept(item);
        }
    }

    public void start() {
        this.thread.start();
    }

    public void close() {
        thread.interrupt();
        while (!thread.getState().equals(Thread.State.TERMINATED)) {
        }
    }
}