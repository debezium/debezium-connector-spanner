/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.StreamEventMetadata;

class PartitionStartEventTest {

    private StreamEventMetadata meta = StreamEventMetadata.newBuilder()
            .withPartitionToken("p-token")
            .withRecordTimestamp(Timestamp.now())
            .withPartitionStartTimestamp(Timestamp.now())
            .withPartitionEndTimestamp(null)
            .withQueryStartedAt(null)
            .withRecordStreamStartedAt(null)
            .withRecordStreamEndedAt(null)
            .withRecordReadAt(null)
            .withTotalStreamTimeMillis(0L)
            .withNumberOfRecordsRead(0L)
            .build();

    @Test
    void testGettersAndEquality() {
        Timestamp ts = Timestamp.ofTimeSecondsAndNanos(123L, 456);
        PartitionStartEvent a = new PartitionStartEvent(
                ts,
                "seq-1",
                List.of("partition-1", "partition-2"),
                true,
                meta);

        PartitionStartEvent b = new PartitionStartEvent(
                ts,
                "seq-1",
                List.of("partition-1", "partition-2"),
                true,
                meta);

        assertEquals(ts, a.getStartTimestamp());
        assertEquals(ts, a.getRecordTimestamp());
        assertEquals("seq-1", a.getRecordSequence());
        assertEquals(List.of("partition-1", "partition-2"), a.getPartitionTokens());
        assertEquals(true, a.isFromInitialPartition());
        assertEquals(meta, a.getMetadata());

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        PartitionStartEvent c = new PartitionStartEvent(
                Timestamp.ofTimeSecondsAndNanos(1L, 2),
                "seq-2",
                List.of("p1", "p2"),
                false,
                meta);
        assertNotEquals(a, c);
    }

    @Test
    void testToStringContainsFields() {
        Timestamp ts = Timestamp.ofTimeSecondsAndNanos(1L, 2);
        PartitionStartEvent e = new PartitionStartEvent(ts, "s", List.of("p1"), true, meta);
        String s = e.toString();
        assertTrue(s.contains("startTimestamp"));
        assertTrue(s.contains("recordSequence='s'"));
        assertTrue(s.contains("partitionToken"));
    }
}
