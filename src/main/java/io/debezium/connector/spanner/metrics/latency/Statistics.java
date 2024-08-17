/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.latency;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * This class provides functionality to calculate statistics:
 * min, max, avg values, percentiles.
 */
public class Statistics extends Metric {
    public Statistics(Duration percentageMetricsClearInterval, Consumer<Throwable> errorConsumer) {
        super(percentageMetricsClearInterval, errorConsumer);
    }
}
