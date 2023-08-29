/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PartitionThreadPoolTest {

    @Test
    void testPartitionThreadPool() {
        PartitionThreadPool partitionThreadPool = new PartitionThreadPool();

        assertTrue(new PartitionThreadPool().getActiveThreads().isEmpty());

        Runnable runnable = () -> {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        partitionThreadPool.submit("Test token1", runnable);
        partitionThreadPool.submit("Test token2", runnable);
        assertEquals(2, partitionThreadPool.getActiveThreads().size());

        partitionThreadPool.stop("Test token1");
        assertEquals(1, partitionThreadPool.getActiveThreads().size());

        partitionThreadPool.shutdown("taskuid");
        assertEquals(0, partitionThreadPool.getActiveThreads().size());
    }
}
