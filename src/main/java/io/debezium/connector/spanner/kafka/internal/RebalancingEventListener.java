/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import io.debezium.connector.spanner.SpannerConnectorTask;
import io.debezium.connector.spanner.exception.SpannerConnectorException;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceEventMetadata;
import io.debezium.connector.spanner.task.utils.ResettableDelayedAction;
import io.debezium.function.BlockingConsumer;

/**
 * Listens for Rebalance Event from the Rebalance-topic,
 * propagates information about it: Member ID, Generation ID,
 * is current task a Leader or not
 * further for processing
 */
public class RebalancingEventListener {

    private static final Logger LOGGER = getLogger(RebalancingEventListener.class);

    private final String consumerGroup;
    private final String topic;
    private final Duration pollDuration;
    private final Duration commitOffsetsTimeout;
    private final long commitOffsetsInterval;
    private final RebalancingConsumerFactory<?, ?> consumerFactory;
    private final java.util.function.Consumer<RuntimeException> errorHandler;
    private volatile Consumer<?, ?> consumer;

    private volatile Thread thread;

    private volatile BlockingConsumer<RebalanceEventMetadata> rebalancingAction;

    private final ResettableDelayedAction resettableDelayedAction;

    private volatile RebalanceEventMetadata lastRebalanceEventMetadata;

    private final SpannerConnectorTask task;

    public RebalancingEventListener(SpannerConnectorTask task, String consumerGroup, String topic,
                                    Duration rebalancingTaskWaitingTimeout,
                                    RebalancingConsumerFactory<?, ?> consumerFactory,
                                    java.util.function.Consumer<RuntimeException> errorHandler) {
        this.task = task;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.pollDuration = Duration.ofMillis(consumerFactory.getConfig().rebalancingPollDuration());
        this.commitOffsetsTimeout = Duration.ofMillis(consumerFactory.getConfig().rebalancingCommitOffsetsTimeout());
        this.commitOffsetsInterval = consumerFactory.getConfig().rebalancingCommitOffsetsInterval();
        this.consumerFactory = consumerFactory;
        this.errorHandler = errorHandler;
        this.resettableDelayedAction = new ResettableDelayedAction("rebalance-delayed-action", rebalancingTaskWaitingTimeout);
    }

    public void listen(BlockingConsumer<RebalanceEventMetadata> action) {
        this.rebalancingAction = action;
        this.consumer = consumerFactory.createSubscribeConsumer(consumerGroup, topic, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // not used
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

                LOGGER.info("Task {} - Rebalance happened", task.getTaskUid());

                ConsumerGroupMetadata meta = consumer.groupMetadata();
                lastRebalanceEventMetadata = new RebalanceEventMetadata(meta.memberId(), meta.generationId(), isLeader(partitions));

                LOGGER.info("Task {} - Rebalance: Waiting for other tasks to connect", task.getTaskUid());
                resettableDelayedAction.set(() -> {
                    LOGGER.info("Task {} -Rebalance finished", task.getTaskUid());

                    try {
                        rebalancingAction.accept(lastRebalanceEventMetadata);
                    }
                    catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                // not used
            }
        });

        thread = new Thread(() -> {
            try {
                long commitOffsetStart = System.currentTimeMillis();
                Instant lastUpdatedTime = Instant.now();
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        consumer.poll(pollDuration);

                        if (commitOffsetStart + commitOffsetsInterval < System.currentTimeMillis()) {
                            consumer.commitSync(commitOffsetsTimeout);
                            commitOffsetStart = System.currentTimeMillis();
                        }
                        if (Instant.now().isAfter(lastUpdatedTime.plus(Duration.ofSeconds(600)))) {
                            LOGGER.info(
                                    "Task Uid {} is still listening to RebalanceEventListener",
                                    this.task.getTaskUid());
                            lastUpdatedTime = Instant.now();
                        }
                    }
                    catch (org.apache.kafka.common.errors.InterruptException e) {
                        return;
                    }
                }

            }
            finally {
                try {
                    consumer.unsubscribe();
                    consumer.close();
                }
                catch (org.apache.kafka.common.errors.InterruptException e) {
                }
            }
        }, "SpannerConnector-RebalancingEventListener");

        thread.setUncaughtExceptionHandler((t, ex) -> {
            errorHandler.accept(new SpannerConnectorException("Error during poll from the Rebalance Topic", ex));
        });

        thread.start();
    }

    private boolean isLeader(Collection<TopicPartition> partitions) {
        return partitions.stream().anyMatch(x -> x.partition() == 0);
    }

    public void shutdown() {
        this.resettableDelayedAction.clear();

        if (this.thread == null) {
            return;
        }

        this.thread.interrupt();

        while (!this.thread.getState().equals(Thread.State.TERMINATED)) {
        }
        this.thread = null;
    }

}
