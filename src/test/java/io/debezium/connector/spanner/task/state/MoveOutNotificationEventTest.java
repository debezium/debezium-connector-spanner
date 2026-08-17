/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

class MoveOutNotificationEventTest {

    @Test
    void testGetters() {
        Timestamp ts = Timestamp.ofTimeMicroseconds(1_000_000L);
        List<String> destinations = List.of("dest1", "dest2");

        MoveOutNotificationEvent event = new MoveOutNotificationEvent("tokenA", ts, destinations);

        assertEquals("tokenA", event.getToken());
        assertEquals(ts, event.getCommitTimestamp());
        assertEquals(destinations, event.getDestinationTokens());
    }

    @Test
    void testEmptyDestinations() {
        Timestamp ts = Timestamp.ofTimeMicroseconds(42L);
        MoveOutNotificationEvent event = new MoveOutNotificationEvent("tok", ts, Collections.emptyList());

        assertEquals("tok", event.getToken());
        assertEquals(ts, event.getCommitTimestamp());
        assertEquals(Collections.emptyList(), event.getDestinationTokens());
    }

    @Test
    void testNullCommitTimestamp() {
        MoveOutNotificationEvent event = new MoveOutNotificationEvent("tok", null, List.of("d1"));
        assertNull(event.getCommitTimestamp());
        assertEquals("tok", event.getToken());
        assertEquals(List.of("d1"), event.getDestinationTokens());
    }
}
