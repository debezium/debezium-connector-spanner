/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.db.model.PartitionKey;

class PartitionThreadPoolTest {

    @Test
    void testPartitionThreadPool() {
        PartitionThreadPool partitionThreadPool = new PartitionThreadPool();

        assertTrue(new PartitionThreadPool().getActiveThreads().isEmpty());

        Runnable runnable = blockingRunnable();
        partitionThreadPool.submit("Test token1", runnable);
        partitionThreadPool.submit("Test token2", runnable);
        assertEquals(2, partitionThreadPool.getActiveThreads().size());

        partitionThreadPool.stop("Test token1");
        assertEquals(1, partitionThreadPool.getActiveThreads().size());

        partitionThreadPool.shutdown("taskuid");
        assertEquals(0, partitionThreadPool.getActiveThreads().size());
    }

    @Test
    void sameTokenInDifferentTvfsRunsConcurrently() {
        PartitionThreadPool partitionThreadPool = new PartitionThreadPool();
        Runnable runnable = blockingRunnable();

        assertTrue(partitionThreadPool.submit("token", "tvfA", runnable));
        assertTrue(partitionThreadPool.submit("token", "tvfB", runnable));
        assertFalse(partitionThreadPool.submit("token", "tvfA", runnable));
        assertEquals(2, partitionThreadPool.getActivePartitions().size());

        partitionThreadPool.stop("token", "tvfA");
        assertEquals(1, partitionThreadPool.getActivePartitions().size());
        assertTrue(partitionThreadPool.getActivePartitions().contains(new PartitionKey("token", "tvfB")));

        partitionThreadPool.shutdown("taskuid");
    }

    private static Runnable blockingRunnable() {
        return () -> {
            try {
                Thread.sleep(10_000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }
}
