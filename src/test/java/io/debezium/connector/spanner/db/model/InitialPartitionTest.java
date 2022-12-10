/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class InitialPartitionTest {

    @Test
    void testIsInitialPartition() {
        assertFalse(InitialPartition.isInitialPartition("token"));
        assertTrue(InitialPartition.isInitialPartition("Parent0"));
    }

}
