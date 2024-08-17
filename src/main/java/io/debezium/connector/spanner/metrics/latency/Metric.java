/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.latency;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public abstract class Metric {

    private final AtomicReference<Duration> minimum = new AtomicReference<>(Duration.ZERO);
    private final AtomicReference<Duration> maximum = new AtomicReference<>(Duration.ZERO);
    private final AtomicReference<Duration> last = new AtomicReference<>(Duration.ZERO);
    private final AtomicReference<Duration> total = new AtomicReference<>(Duration.ZERO);
    private final AtomicReference<Duration> average = new AtomicReference<>(Duration.ZERO);
    private final QuantileMeter quantileMeter;
    private final AtomicLong count = new AtomicLong();

    public Metric(Duration percentageMetricsClearInterval, Consumer<Throwable> errorConsumer) {
        this.quantileMeter = new QuantileMeter(percentageMetricsClearInterval, errorConsumer);
    }

    /**
     * Resets the duration metric
     */
    public void reset() {
        minimum.set(Duration.ZERO);
        maximum.set(Duration.ZERO);
        last.set(Duration.ZERO);
        total.set(Duration.ZERO);
        average.set(Duration.ZERO);
        quantileMeter.reset();
        count.set(0);
    }

    public void start() {
        quantileMeter.start();
    }

    public void shutdown() {
        quantileMeter.shutdown();
    }

    /**
     * Sets the last duration-based value for the histogram.
     *
     * @param lastDuration last duration
     */
    void set(Duration lastDuration) {
        if (lastDuration.compareTo(minimum.get()) < 0) {
            minimum.set(lastDuration);
        }

        if (lastDuration.compareTo(maximum.get()) > 0) {
            maximum.set(lastDuration);
        }

        total.accumulateAndGet(lastDuration, Duration::plus);

        average.set(
                average.get()
                        .multipliedBy(count.get() - 1)
                        .plus(lastDuration)
                        .dividedBy(count.get()));

        last.set(lastDuration);

        quantileMeter.addValue((double) lastDuration.toMillis());
    }

    public synchronized void update(long value) {
        set(Duration.ofMillis(value));
    }

    public Duration getMinValue() {
        return count.get() == 0 ? null : minimum.get();
    }

    public Duration getMaxValue() {
        return count.get() == 0 ? null : maximum.get();
    }

    public Duration getAverageValue() {
        return count.get() == 0 ? null : average.get();
    }

    public Duration getLastValue() {
        return count.get() == 0 ? null : last.get();
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
