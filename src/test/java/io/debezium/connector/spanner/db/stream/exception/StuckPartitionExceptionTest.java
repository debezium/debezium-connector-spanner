/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class StuckPartitionExceptionTest {

    @Test
    void testConstructor() {
        StuckPartitionException actualStuckPartitionException = new StuckPartitionException("Msg");
        assertNull(actualStuckPartitionException.getCause());
        assertEquals(0, actualStuckPartitionException.getSuppressed().length);
        assertEquals("Spanner partition querying has been stuck: Msg", actualStuckPartitionException.getMessage());
        assertEquals("Spanner partition querying has been stuck: Msg",
                actualStuckPartitionException.getLocalizedMessage());
    }
}
