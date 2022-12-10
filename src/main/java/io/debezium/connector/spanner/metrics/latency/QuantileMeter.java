/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.latency;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.lang3.ArrayUtils;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;

import io.debezium.connector.spanner.task.utils.TimeoutMeter;

/**
 * Utility to calculate quantiles for streaming data
 */
public class QuantileMeter {

    private static final int QUEUE_SIZE = 1000;
    private static final double[] QUANTILES = { 0.5, 0.95, 0.99 };
    private static final Double[] EMPTY_VALUES = { null, null, null };
    private final BlockingQueue<Double> queue = new LinkedBlockingQueue<>(QUEUE_SIZE);

    private final Thread thread;
    private final DDSketch sketch = DDSketches.unboundedDense(0.01);
    private final Consumer<Throwable> errorConsumer;

    public QuantileMeter(Duration clearInterval, Consumer<Throwable> errorConsumer) {
        this.errorConsumer = errorConsumer;

        this.thread = new Thread(() -> {

            TimeoutMeter timeoutMeter = null;
            if (!clearInterval.isZero()) {
                timeoutMeter = TimeoutMeter.setTimeout(clearInterval);
            }

            while (!Thread.currentThread().isInterrupted()) {
                Double pollValue;
                try {
                    pollValue = queue.poll(100, TimeUnit.MILLISECONDS);

                    if (timeoutMeter != null && timeoutMeter.isExpired()) {
                        timeoutMeter = TimeoutMeter.setTimeout(clearInterval);
                        sketch.clear();
                    }

                    if (pollValue == null) {
                        continue;
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                accept(pollValue);
            }
        }, "SpannerConnector-QuantileMeter");

        this.thread.setUncaughtExceptionHandler((t, ex) -> this.errorConsumer.accept(ex));

    }

    public void start() {
        this.thread.start();
    }

    public boolean addValue(double value) {
        return queue.offer(value);
    }

    private synchronized void accept(double value) {
        sketch.accept(value);
    }

    public synchronized Double getValueAtQuantile(double quantile) {
        return sketch.isEmpty() ? null : sketch.getValueAtQuantile(quantile);
    }

    public synchronized Double[] getValuesAtQuantiles() {
        return sketch.isEmpty() ? EMPTY_VALUES : ArrayUtils.toObject(sketch.getValuesAtQuantiles(QUANTILES));
    }

    public synchronized void reset() {
        queue.clear();
        sketch.clear();
    }

    public void shutdown() {
        this.reset();
        this.thread.interrupt();
    }

    // for testing
    double getCount() {
        return sketch.getCount();
    }
}
