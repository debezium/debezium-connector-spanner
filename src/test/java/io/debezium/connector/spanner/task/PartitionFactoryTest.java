/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

class PartitionFactoryTest {

    private static final Timestamp START = Timestamp.parseTimestamp("2026-01-01T00:00:00Z");
    private static final Timestamp OFFSET = Timestamp.parseTimestamp("2026-01-01T00:05:00Z");

    private PartitionState buildPartitionState(String token) {
        return PartitionState.builder()
                .token(token)
                .startTimestamp(START)
                .endTimestamp(null)
                .state(PartitionStateEnum.READY_FOR_STREAMING)
                .parents(Set.of())
                .build();
    }

    private PartitionFactory buildFactory(Map<Map<String, String>, Map<String, Object>> offsets) {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        when(reader.offsets(any())).thenAnswer(invocation -> offsets);

        MetricsEventPublisher metricsPublisher = new MetricsEventPublisher();
        PartitionOffsetProvider offsetProvider = new PartitionOffsetProvider(reader, metricsPublisher);
        return new PartitionFactory(offsetProvider, metricsPublisher);
    }

    @Test
    void testGetPartitionsBatchResolution() {
        Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(Map.of("partitionToken", "token1"), Map.of("offset", OFFSET.toString()));
        offsets.put(Map.of("partitionToken", "token2"), Map.of("offset", OFFSET.toString()));

        PartitionFactory factory = buildFactory(offsets);

        List<PartitionState> states = List.of(buildPartitionState("token1"), buildPartitionState("token2"));
        Map<String, Partition> result = factory.getPartitions(states);

        assertEquals(2, result.size());
        assertNotNull(result.get("token1"));
        assertNotNull(result.get("token2"));
        assertEquals(OFFSET, result.get("token1").getStartTimestamp());
        assertEquals(OFFSET, result.get("token2").getStartTimestamp());
    }

    @Test
    void testGetPartitionsWithNullOffset() {
        PartitionFactory factory = buildFactory(new HashMap<>());

        List<PartitionState> states = List.of(buildPartitionState("token1"));
        Map<String, Partition> result = factory.getPartitions(states);

        assertEquals(1, result.size());
        assertEquals(START, result.get("token1").getStartTimestamp());
    }

    @Test
    void testGetPartitionsWithIncorrectOffset() {
        Timestamp beforeStart = Timestamp.parseTimestamp("2025-12-31T00:00:00Z");
        Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(Map.of("partitionToken", "token1"), Map.of("offset", beforeStart.toString()));

        PartitionFactory factory = buildFactory(offsets);

        List<PartitionState> states = List.of(buildPartitionState("token1"));
        Map<String, Partition> result = factory.getPartitions(states);

        assertEquals(1, result.size());
        assertEquals(START, result.get("token1").getStartTimestamp());
    }
}
