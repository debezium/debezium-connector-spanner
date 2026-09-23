/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.PartitionKey;

class PartitionStateTest {

    private static final Timestamp TS = Timestamp.parseTimestamp("2026-01-01T00:00:00Z");

    @Test
    void sameTokenDifferentTvfAreNotEqual() {
        PartitionState noTvf = state("token", null);
        PartitionState tvfA = state("token", "tvfA");
        PartitionState tvfB = state("token", "tvfB");

        assertNotEquals(tvfA, tvfB);
        assertNotEquals(tvfA, noTvf);
        assertEquals(noTvf, state("token", null));
    }

    @Test
    void sameTokenDifferentTvfHaveDifferentHashCodes() {
        PartitionState tvfA = state("token", "tvfA");
        PartitionState tvfB = state("token", "tvfB");

        assertNotEquals(tvfA.hashCode(), tvfB.hashCode());
    }

    @Test
    void getKeyCombinesTokenAndTvf() {
        PartitionState state = state("token", "tvfA");

        assertEquals(new PartitionKey("token", "tvfA"), state.getKey());
        assertEquals(new PartitionKey("token", null), state("token", null).getKey());
    }

    @Test
    void compareToAccountsForTvfBeforeState() {
        PartitionState noTvf = state("token", null);
        PartitionState tvfA = state("token", "tvfA");

        assertTrue(noTvf.compareTo(tvfA) < 0);
        assertTrue(tvfA.compareTo(noTvf) > 0);
    }

    private static PartitionState state(String token, String tvfName) {
        return PartitionState.builder()
                .token(token)
                .tvfName(tvfName)
                .startTimestamp(TS)
                .state(PartitionStateEnum.CREATED)
                .parents(Set.of())
                .build();
    }
}
