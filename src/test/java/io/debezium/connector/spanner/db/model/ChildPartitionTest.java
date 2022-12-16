/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

class ChildPartitionTest {

    @Test
    void testConstructor() {
        Set<String> parentTokens = new HashSet<>();
        parentTokens.add("p1");
        parentTokens.add("p2");
        ChildPartition actualChildPartition = new ChildPartition("token", parentTokens);
        assertSame(parentTokens, actualChildPartition.getParentTokens());
        assertEquals("token", actualChildPartition.getToken());
        assertEquals("ChildPartition{childToken='token', parentTokens=[p1, p2]}", actualChildPartition.toString());
    }
}
