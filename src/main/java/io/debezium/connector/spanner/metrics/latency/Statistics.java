/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.latency;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * This class provides functionality to calculate statistics:
 * min, max, avg values, percentiles.
 */
public class Statistics {
    private final AtomicLong minValue = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxValue = new AtomicLong(0);
    private final AtomicDouble averageValue = new AtomicDouble(0);
    private final AtomicLong lastValue = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong count = new AtomicLong(0);
    private final QuantileMeter quantileMeter;

    public Statistics(Duration percentageMetricsClearInterval, Consumer<Throwable> errorConsumer) {
        this.quantileMeter = new QuantileMeter(percentageMetricsClearInterval, errorConsumer);
    }

    public synchronized void reset() {
        minValue.set(Long.MAX_VALUE);
        maxValue.set(0);
        averageValue.set(0);
        lastValue.set(Long.MAX_VALUE);
        count.set(0);
        quantileMeter.reset();
    }

    public void start() {
        quantileMeter.start();
    }

    public void shutdown() {
        quantileMeter.shutdown();
    }

    void set(Duration lastDuration) {
        long value = lastDuration.toMillis();
        if (minValue.get() > value) {
            minValue.set(value);
        }

        if (maxValue.get() < value) {
            maxValue.set(value);
        }

        count.incrementAndGet();

        averageValue.set((averageValue.get() * (count.get() - 1) + value) / count.get());

        lastValue.set(value);

        quantileMeter.addValue((double) value);
    }

    public synchronized void update(long value) {
        set(Duration.ofMillis(value));
    }

    public Long getMinValue() {
        return count.get() == 0 ? null : minValue.get();
    }

    public Long getMaxValue() {
        return count.get() == 0 ? null : maxValue.get();
    }

    public Double getAverageValue() {
        return count.get() == 0 ? null : averageValue.get();
    }

    public Long getLastValue() {
        return count.get() == 0 ? null : lastValue.get();
    }

    public Double getValueAtP50() {
        return quantileMeter.getValueAtQuantile(0.5);
    }

    public Double getValueAtP95() {
        return quantileMeter.getValueAtQuantile(0.95);
    }

    public Double getValueAtP99() {
        return quantileMeter.getValueAtQuantile(0.99);
    }
}
