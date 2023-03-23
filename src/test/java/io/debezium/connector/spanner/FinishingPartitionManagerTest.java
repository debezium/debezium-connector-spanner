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
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void onPartitionFinishEvent() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void forceFinish() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);
        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.forceFinish("testToken");

        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void withoutRegistration() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");
    }

    @Test
    void multipleCommitFinishEventFirst() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaab");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaac");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitCommitFirst1() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaab");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaac");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitCommitFirst2() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaab");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaac");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitCommitOutOfOrder() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaac");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaab");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");
        finishingPartitionManager.onPartitionFinishEvent("testToken");
        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitNoEvents() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

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
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

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
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.commitRecord("testToken", "recordUid3");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");
    }
}
