/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.PartitionKey;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

class PartitionOffsetProviderTest {

    @Test
    void testGetOffsetsReturnsOffsets() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        MetricsEventPublisher metricsPublisher = new MetricsEventPublisher();

        PartitionState partitionState = partitionState("token1", null);
        Timestamp expected = Timestamp.parseTimestamp("2026-01-01T00:05:00Z");
        Map<String, String> partitionKey = new LinkedHashMap<>();
        partitionKey.put("partitionToken", "token1");
        Map<String, Object> offsetValue = Map.of("offset", expected.toString());
        Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(partitionKey, offsetValue);

        when(reader.offsets(any())).thenAnswer(invocation -> offsets);

        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, metricsPublisher, 30000L);
        Map<PartitionKey, Timestamp> result = provider.getOffsets(List.of(partitionState));

        assertEquals(1, result.size());
        assertEquals(expected, result.get(partitionState.getKey()));
    }

    @Test
    void testGetOffsetsReturnsEmptyOnNull() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        MetricsEventPublisher metricsPublisher = new MetricsEventPublisher();

        when(reader.offsets(any())).thenAnswer(invocation -> null);

        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, metricsPublisher, 30000L);
        Map<PartitionKey, Timestamp> result = provider.getOffsets(List.of(partitionState("token1", null)));

        assertTrue(result.isEmpty());
    }

    @Test
    void testGetOffsetsSkipsEntryWithoutPartitionToken() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        MetricsEventPublisher metricsPublisher = new MetricsEventPublisher();
        Timestamp offset = Timestamp.parseTimestamp("2026-01-01T00:05:00Z");

        when(reader.offsets(any())).thenReturn(Map.of(
                Map.of("tvfName", "tvfA"), Map.of("offset", offset.toString())));

        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, metricsPublisher, 30000L);

        assertTrue(provider.getOffsets(List.of(partitionState("token1", "tvfA"))).isEmpty());
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
        Map<PartitionKey, Timestamp> result = provider.getOffsets(List.of(partitionState("token1", null)));

        assertTrue(result.isEmpty());
    }

    @Test
    void testGetOffsetsDistinguishesPartitionsByTvfName() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        MetricsEventPublisher metricsPublisher = new MetricsEventPublisher();

        PartitionState tvfA = partitionState("token1", "tvfA");
        PartitionState tvfB = partitionState("token1", "tvfB");

        Timestamp offsetA = Timestamp.parseTimestamp("2026-01-01T00:05:00Z");
        Timestamp offsetB = Timestamp.parseTimestamp("2026-01-01T00:10:00Z");

        Map<String, String> keyA = Map.of("partitionToken", "token1", "tvfName", "tvfA");
        Map<String, String> keyB = Map.of("partitionToken", "token1", "tvfName", "tvfB");
        Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(keyA, Map.of("offset", offsetA.toString()));
        offsets.put(keyB, Map.of("offset", offsetB.toString()));

        when(reader.offsets(any())).thenAnswer(invocation -> offsets);

        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, metricsPublisher, 30000L);
        Map<PartitionKey, Timestamp> result = provider.getOffsets(List.of(tvfA, tvfB));

        assertEquals(2, result.size());
        assertEquals(offsetA, result.get(tvfA.getKey()));
        assertEquals(offsetB, result.get(tvfB.getKey()));
    }

    @Test
    void testGetOffsetsSupportsMixedNullAndNamedTvfPartitions() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        MetricsEventPublisher metricsPublisher = new MetricsEventPublisher();

        PartitionState mutableWithoutTvf = partitionState("shared-token", null);
        PartitionState mutableWithTvf = partitionState("shared-token", "tvfA");
        Timestamp offsetWithoutTvf = Timestamp.parseTimestamp("2026-01-01T00:05:00Z");
        Timestamp offsetWithTvf = Timestamp.parseTimestamp("2026-01-01T00:10:00Z");
        Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(Map.of("partitionToken", "shared-token"), Map.of("offset", offsetWithoutTvf.toString()));
        offsets.put(Map.of("partitionToken", "shared-token", "tvfName", "tvfA"), Map.of("offset", offsetWithTvf.toString()));
        when(reader.offsets(any())).thenAnswer(invocation -> offsets);

        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, metricsPublisher, 30000L);
        Map<PartitionKey, Timestamp> result = provider.getOffsets(List.of(mutableWithoutTvf, mutableWithTvf));

        assertEquals(2, result.size());
        assertEquals(offsetWithoutTvf, result.get(mutableWithoutTvf.getKey()));
        assertEquals(offsetWithTvf, result.get(mutableWithTvf.getKey()));
    }

    @Test
    void testGetOffsetUsesTvfNameForSourcePartitionKey() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        MetricsEventPublisher metricsPublisher = new MetricsEventPublisher();

        PartitionState tvfA = partitionState("token1", "tvfA");
        Timestamp offsetA = Timestamp.parseTimestamp("2026-01-01T00:05:00Z");
        Map<String, String> expectedKey = Map.of("partitionToken", "token1", "tvfName", "tvfA");

        when(reader.offset(anyMap())).thenAnswer(invocation -> {
            Map<String, String> requested = invocation.getArgument(0);
            assertEquals(expectedKey, requested);
            return Map.of("offset", offsetA.toString());
        });

        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, metricsPublisher, 30000L);
        Timestamp result = provider.getOffset(tvfA);

        assertNotNull(result);
        assertEquals(offsetA, result);
    }

    private static PartitionState partitionState(String token, String tvfName) {
        return PartitionState.builder()
                .token(token)
                .tvfName(tvfName)
                .state(PartitionStateEnum.RUNNING)
                .parents(Set.of())
                .build();
    }
}
