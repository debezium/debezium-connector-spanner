/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.StreamEventMetadata;

class PartitionEndEventTest {

    StreamEventMetadata meta = StreamEventMetadata.newBuilder()
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
        PartitionEndEvent a = new PartitionEndEvent(
                ts,
                "seq-1",
                "partition-1",
                meta);

        PartitionEndEvent b = new PartitionEndEvent(
                ts,
                "seq-1",
                "partition-1",
                meta);

        assertEquals(ts, a.getEndTimestamp());
        assertEquals(ts, a.getRecordTimestamp());
        assertEquals("seq-1", a.getRecordSequence());
        assertEquals("partition-1", a.getPartitionToken());
        assertEquals(meta, a.getMetadata());

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        PartitionEndEvent c = new PartitionEndEvent(
                Timestamp.ofTimeSecondsAndNanos(1L, 2),
                "seq-2",
                "partition-2",
                meta);
        assertNotEquals(a, c);
    }

    @Test
    void toStringContainsFields() {
        Timestamp ts = Timestamp.ofTimeSecondsAndNanos(1L, 2);
        PartitionEndEvent e = new PartitionEndEvent(ts, "s", "p", meta);
        String s = e.toString();
        // must contain some field names/values
        assertEquals(true, s.contains("endTimestamp"));
        assertEquals(true, s.contains("recordSequence='s'"));
        assertEquals(true, s.contains("partitionToken"));
    }
}
