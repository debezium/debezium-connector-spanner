/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.latency;

import java.time.Duration;
import java.util.function.Consumer;

import org.apache.commons.lang3.ArrayUtils;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;

import io.debezium.connector.spanner.task.utils.TimeoutMeter;

/**
 * Utility to calculate quantiles for streaming data
 */
public class QuantileMeter {

    private static final double[] QUANTILES = { 0.5, 0.95, 0.99 };
    private static final Double[] EMPTY_VALUES = { null, null, null };

    private final DDSketch sketch = DDSketches.unboundedDense(0.01);
    private final Duration clearInterval;
    private TimeoutMeter timeoutMeter;

    public QuantileMeter(Duration clearInterval, Consumer<Throwable> errorConsumer) {
        this.clearInterval = clearInterval;
    }

    public void start() {
        if (clearInterval != null && !clearInterval.isZero()) {
            timeoutMeter = TimeoutMeter.setTimeout(clearInterval);
        }
    }

    public synchronized void addValue(double value) {
        if (timeoutMeter != null && timeoutMeter.isExpired()) {
            timeoutMeter = TimeoutMeter.setTimeout(clearInterval);
            sketch.clear();
        }
        sketch.accept(value);
    }

    public synchronized Double getValueAtQuantile(double quantile) {
        return sketch.isEmpty() ? null : sketch.getValueAtQuantile(quantile);
    }

    public synchronized Double[] getValuesAtQuantiles() {
        return sketch.isEmpty() ? EMPTY_VALUES : ArrayUtils.toObject(sketch.getValuesAtQuantiles(QUANTILES));
    }

    public synchronized void reset() {
        sketch.clear();
    }

    public void shutdown() {
        this.reset();
    }

    // for testing
    double getCount() {
        return sketch.getCount();
    }
}
