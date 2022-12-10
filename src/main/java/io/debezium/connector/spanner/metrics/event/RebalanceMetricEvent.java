/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

/**
 * Tracks number of actual and expected responses during the last Rebalance Event
 */
public class RebalanceMetricEvent implements MetricEvent {
    private final int rebalanceAnswersActual;
    private final int rebalanceAnswersExpected;

    public RebalanceMetricEvent(int rebalanceAnswersActual, int rebalanceAnswersExpected) {
        this.rebalanceAnswersActual = rebalanceAnswersActual;
        this.rebalanceAnswersExpected = rebalanceAnswersExpected;
    }

    public int getRebalanceAnswersActual() {
        return rebalanceAnswersActual;
    }

    public int getRebalanceAnswersExpected() {
        return rebalanceAnswersExpected;
    }

}
