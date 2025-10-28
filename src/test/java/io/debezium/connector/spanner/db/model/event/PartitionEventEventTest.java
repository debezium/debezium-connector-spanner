/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.StreamEventMetadata;

class PartitionEventEventTest {

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
        PartitionEventEvent a = new PartitionEventEvent(
                ts,
                "seq-1",
                "partition-1",
                List.of("src1", "src2"),
                List.of("dst1"),
                meta);

        PartitionEventEvent b = new PartitionEventEvent(
                ts,
                "seq-1",
                "partition-1",
                List.of("src1", "src2"),
                List.of("dst1"),
                meta);

        assertEquals(ts, a.getCommitTimestamp());
        assertEquals(ts, a.getRecordTimestamp());
        assertEquals("seq-1", a.getRecordSequence());
        assertEquals("partition-1", a.getPartitionToken());
        assertEquals(List.of("src1", "src2"), a.getSourcePartitions());
        assertEquals(List.of("dst1"), a.getDestinationPartitions());
        assertEquals(meta, a.getMetadata());

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        PartitionEventEvent c = new PartitionEventEvent(
                Timestamp.ofTimeSecondsAndNanos(1L, 2),
                "seq-2",
                "partition-2",
                List.of("s"),
                List.of("d"),
                meta);
        assertNotEquals(a, c);
    }

    @Test
    void testToStringContainsFields() {
        Timestamp ts = Timestamp.ofTimeSecondsAndNanos(1L, 2);
        PartitionEventEvent e = new PartitionEventEvent(ts, "s", "p", List.of("x"), List.of("y"), meta);
        String s = e.toString();
        // must contain some field names/values
        assertEquals(true, s.contains("commitTimestamp"));
        assertEquals(true, s.contains("recordSequence='s'"));
        assertEquals(true, s.contains("partitionToken"));
    }
}
