/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.metrics.event.LatencyMetricEvent;
import io.debezium.connector.spanner.metrics.event.MetricEvent;
import io.debezium.connector.spanner.metrics.latency.LatencyCalculator;
import io.debezium.connector.spanner.processor.SourceRecordUtils;

/**
 * Publishes {@link MetricEvent}
 */
public class MetricsEventPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsEventPublisher.class);
    private final Map<Class<? extends MetricEvent>, Consumer<? extends MetricEvent>> subscribes = new ConcurrentHashMap<>();

    public <T extends MetricEvent> void publishMetricEvent(T metricEvent) {
        Consumer<T> consumer = (Consumer<T>) subscribes.get(metricEvent.getClass());
        if (consumer != null) {
            try {
                consumer.accept(metricEvent);
            }
            catch (Exception ex) {
                LOGGER.warn("Failed to process metric event: " + metricEvent, ex);
            }
        }
    }

    public <T extends MetricEvent> void subscribe(Class<T> clazz, Consumer<T> consumer) {
        if (subscribes.containsKey(clazz)) {
            throw new IllegalStateException();
        }
        subscribes.put(clazz, consumer);
    }

    public void logLatency(SourceRecord sourceRecord) {
        if (!SourceRecordUtils.isDataChangeRecord(sourceRecord)) {
            return;
        }

        Long totalLatency = LatencyCalculator.getTotalLatency(sourceRecord);
        if (totalLatency != null && totalLatency > 300000) {
            LOGGER.debug("Published very high total latency for source record {}:{}", sourceRecord, totalLatency);
        }

        Long readToEmitLatency = LatencyCalculator.getReadToEmitLatency(sourceRecord);
        if (readToEmitLatency != null && readToEmitLatency > 300000) {
            LOGGER.debug("Published very high readToEmit latency for source record {}:{}", sourceRecord, readToEmitLatency);
        }

        Long spannerLatency = LatencyCalculator.getSpannerLatency(sourceRecord);
        if (spannerLatency != null && spannerLatency > 300000) {
            LOGGER.debug("Published very high spannerLatnency latency for source record {}:{}", sourceRecord, spannerLatency);
        }

        Long commitToEmitLatency = LatencyCalculator.getCommitToEmitLatency(sourceRecord);
        if (commitToEmitLatency != null && commitToEmitLatency > 300000) {
            LOGGER.debug("Published very high spannerLatnency latency for source record {}:{}", sourceRecord, commitToEmitLatency);
        }

        Long commitToPublishLatency = LatencyCalculator.getCommitToPublishLatency(sourceRecord);
        if (commitToPublishLatency != null && commitToPublishLatency > 300000) {
            LOGGER.debug("Published very high commitToPublishLatency latency for source record {}:{}", sourceRecord, commitToPublishLatency);
        }

        Long emitToPublishLatency = LatencyCalculator.getEmitToPublishLatency(sourceRecord);
        if (emitToPublishLatency != null && emitToPublishLatency > 300000) {
            LOGGER.debug("Published very high emitToPublishLatency latency for source record {}:{}", sourceRecord, emitToPublishLatency);
        }

        Long ownConnectorLatency = LatencyCalculator.getOwnConnectorLatency(sourceRecord);
        if (ownConnectorLatency != null && ownConnectorLatency > 300000) {
            LOGGER.debug("Published very high ownConnectorLatency latency for source record {}:{}", sourceRecord, ownConnectorLatency);
        }

        Long lowWatermarkLag = LatencyCalculator.getLowWatermarkLag(sourceRecord);
        if (lowWatermarkLag != null && lowWatermarkLag > 300000) {
            LOGGER.info("Published very high lowWatermarkLag latency for source record {}:{}", sourceRecord, lowWatermarkLag);
        }

        this.publishMetricEvent(new LatencyMetricEvent(totalLatency, readToEmitLatency, spannerLatency,
                commitToEmitLatency, commitToPublishLatency, emitToPublishLatency, lowWatermarkLag, ownConnectorLatency));
    }

}
