/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.storage.OffsetStorageReader;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.connector.spanner.context.offset.PartitionOffset;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.OffsetReceivingTimeMetricEvent;

/**
 * Retrieves offsets from Kafka Connect
 * and publishes appropriate metrics
 */
public class PartitionOffsetProvider {
    private final OffsetStorageReader offsetStorageReader;
    private final MetricsEventPublisher metricsEventPublisher;

    public PartitionOffsetProvider(OffsetStorageReader offsetStorageReader, MetricsEventPublisher metricsEventPublisherr) {
        this.offsetStorageReader = offsetStorageReader;
        this.metricsEventPublisher = metricsEventPublisherr;
    }

    public Timestamp getOffset(String token) {
        Instant startTime = Instant.now();

        Map<String, ?> result = this.offsetStorageReader.offset(new SpannerPartition(token).getSourcePartition());

        if (result == null) {
            return null;
        }

        metricsEventPublisher.publishMetricEvent(OffsetReceivingTimeMetricEvent.from(startTime));

        return PartitionOffset.extractOffset(result);
    }

    public Map<String, String> getOffsetMap(String token) {

        Map<String, ?> result = this.offsetStorageReader.offset(new SpannerPartition(token).getSourcePartition());

        if (result == null) {
            return Map.of();
        }
        return (Map<String, String>) result;
    }

    public Map<String, Timestamp> getOffsets(Collection<String> partitions) {
        Instant startTime = Instant.now();

        List<Map<String, String>> partitionsMapList = partitions.stream()
                .map(token -> new SpannerPartition(token).getSourcePartition())
                .collect(Collectors.toList());

        Map<Map<String, String>, Map<String, Object>> result = this.offsetStorageReader.offsets(partitionsMapList);

        if (result == null) {
            return Map.of();
        }

        metricsEventPublisher.publishMetricEvent(OffsetReceivingTimeMetricEvent.from(startTime));

        Map<String, Timestamp> map = new HashMap<>();

        for (Map.Entry<Map<String, String>, Map<String, Object>> entry : result.entrySet()) {
            map.put(SpannerPartition.extractToken(entry.getKey()),
                    PartitionOffset.extractOffset(entry.getValue()));
        }

        return map;
    }

}
