/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

/**
 * Used to update metric values of the Spanner Connector.
 *
 * {@link io.debezium.connector.spanner.metrics.SpannerMeter} subscribes to such events
 * for further processing.
 */
public interface MetricEvent {
}
