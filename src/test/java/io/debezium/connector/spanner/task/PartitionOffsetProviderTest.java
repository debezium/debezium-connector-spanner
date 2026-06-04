/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

class PartitionOffsetProviderTest {

    @Test
    void testGetOffsetsReturnsOffsets() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        MetricsEventPublisher metricsPublisher = new MetricsEventPublisher();

        Timestamp expected = Timestamp.parseTimestamp("2026-01-01T00:05:00Z");
        Map<String, String> partitionKey = Map.of("partitionToken", "token1");
        Map<String, Object> offsetValue = Map.of("offset", expected.toString());
        Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(partitionKey, offsetValue);

        when(reader.offsets(any())).thenAnswer(invocation -> offsets);

        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, metricsPublisher, 30000L);
        Map<String, Timestamp> result = provider.getOffsets(List.of("token1"));

        assertEquals(1, result.size());
        assertEquals(expected, result.get("token1"));
    }

    @Test
    void testGetOffsetsReturnsEmptyOnNull() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        MetricsEventPublisher metricsPublisher = new MetricsEventPublisher();

        when(reader.offsets(any())).thenAnswer(invocation -> null);

        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, metricsPublisher, 30000L);
        Map<String, Timestamp> result = provider.getOffsets(List.of("token1"));

        assertTrue(result.isEmpty());
    }

    @Test
    void testGetOffsetsReturnsEmptyOnTimeout() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        MetricsEventPublisher metricsPublisher = new MetricsEventPublisher();

        when(reader.offsets(any())).thenAnswer(invocation -> {
            Thread.sleep(60_000);
            return null;
        });

        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, metricsPublisher, 30000L);
        Map<String, Timestamp> result = provider.getOffsets(List.of("token1"));

        assertTrue(result.isEmpty());
    }
}
