/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.function.BlockingConsumer;

class FinishingPartitionManagerTest {

    @Test
    void commitRecord() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken", "recordUid1");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        finishingPartitionManager.commitRecord("testToken", "recordUid1");

        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void onPartitionFinishEvent() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken", "recordUid1");

        finishingPartitionManager.commitRecord("testToken", "recordUid1");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void forceFinish() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken", "recordUid1");

        finishingPartitionManager.forceFinish("testToken");

        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void withoutRegistration() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(consumer);

        finishingPartitionManager.newRecord("testToken", "recordUid1");

        finishingPartitionManager.commitRecord("testToken", "recordUid1");

        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");
    }

    @Test
    void multipleCommitFinishEventFirst() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken", "recordUid1");

        finishingPartitionManager.newRecord("testToken", "recordUid2");

        finishingPartitionManager.newRecord("testToken", "recordUid3");

        finishingPartitionManager.commitRecord("testToken", "recordUid1");

        finishingPartitionManager.commitRecord("testToken", "recordUid2");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.commitRecord("testToken", "recordUid3");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitCommitFirst1() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken", "recordUid1");

        finishingPartitionManager.newRecord("testToken", "recordUid2");

        finishingPartitionManager.newRecord("testToken", "recordUid3");

        finishingPartitionManager.commitRecord("testToken", "recordUid1");

        finishingPartitionManager.commitRecord("testToken", "recordUid2");

        finishingPartitionManager.commitRecord("testToken", "recordUid3");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitCommitFirst2() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken", "recordUid1");

        finishingPartitionManager.newRecord("testToken", "recordUid2");

        finishingPartitionManager.commitRecord("testToken", "recordUid1");

        finishingPartitionManager.commitRecord("testToken", "recordUid2");

        finishingPartitionManager.newRecord("testToken", "recordUid3");

        finishingPartitionManager.commitRecord("testToken", "recordUid3");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitNoEvents() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(consumer);

        finishingPartitionManager.registerPartition("testToken");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitNoEventsWithWrongCommitCall() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.commitRecord("testToken", "recordUid3");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitNoEventsWithWrongCommitCallOnly() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.commitRecord("testToken", "recordUid3");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");
    }
}
