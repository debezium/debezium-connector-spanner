/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static io.debezium.connector.spanner.task.LoggerUtils.debug;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import io.debezium.DebeziumException;
import io.debezium.connector.spanner.exception.SpannerConnectorException;
import io.debezium.connector.spanner.function.BlockingBiConsumer;
import io.debezium.connector.spanner.kafka.event.proto.SyncEventProtos;
import io.debezium.connector.spanner.kafka.internal.model.SyncEventMetadata;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.kafka.internal.proto.SyncEventFromProtoMapper;
import io.debezium.util.Stopwatch;

/** Consumes messages from the Sync Topic */
public class TaskSyncEventListener {
    private static final Logger LOGGER = getLogger(TaskSyncEventListener.class);
    private final String consumerGroup;
    private final String topic;
    private final boolean seekBackToPreviousEpoch;
    private final Duration pollDuration;
    private final Duration commitOffsetsTimeout;
    private final long commitOffsetsInterval;
    private final SyncEventConsumerFactory<String, byte[]> consumerFactory;
    private final List<BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata>> eventConsumers = new ArrayList<>();
    private final java.util.function.Consumer<RuntimeException> errorHandler;
    private final Duration pollInterval = Duration.ofMillis(300000);

    private volatile Thread thread;

    public TaskSyncEventListener(
                                 String consumerGroup,
                                 String topic,
                                 SyncEventConsumerFactory<String, byte[]> consumerFactory,
                                 boolean seekBackToPreviousEpoch,
                                 java.util.function.Consumer<RuntimeException> errorHandler) {

        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.seekBackToPreviousEpoch = seekBackToPreviousEpoch;
        this.pollDuration = Duration.ofMillis(consumerFactory.getConfig().syncPollDuration());
        this.commitOffsetsTimeout = Duration.ofMillis(consumerFactory.getConfig().syncCommitOffsetsTimeout());
        this.commitOffsetsInterval = consumerFactory.getConfig().syncCommitOffsetsInterval();
        this.consumerFactory = consumerFactory;
        this.errorHandler = errorHandler;
    }

    public void subscribe(BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer) {
        eventConsumers.add(eventConsumer);
    }

    public void unsubscribe(BiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer) {
        eventConsumers.remove(eventConsumer);
    }

    public void start() throws InterruptedException {
        // or take all partition list and sub to them

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        List<TopicPartition> assignment = List.of(topicPartition);

        Consumer<String, byte[]> consumer = consumerFactory.createConsumer(consumerGroup);
        consumer.assign(assignment);

        Long endOffset = consumer.endOffsets(assignment).get(topicPartition);
        Long beginOffset = consumer.beginningOffsets(assignment).get(topicPartition);

        long startOffset = Math.max(endOffset - 1, beginOffset);

        try {

            if (endOffset == startOffset) {
                LOGGER.info("task {}, listen: Sync topic is empty, so initial sync is finished", consumerGroup);
                for (BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer : eventConsumers) {
                    eventConsumer.accept(
                            null, SyncEventMetadata.builder().canInitiateRebalancing(true).build());
                }
            }
            else {
                LOGGER.info("Task {}, listen: read last message with start offset {} and end offset {}", consumerGroup, startOffset, endOffset);
                try {
                    LOGGER.info("Task {}, seeking back to end offset {}", consumerGroup, endOffset);
                    consumer.seek(topicPartition, startOffset);
                    LOGGER.info("Task {}, seeking back to previous epoch", consumerGroup);
                    seekBackToPreviousEpoch(consumer, topicPartition, beginOffset, startOffset);
                }
                catch (org.apache.kafka.common.errors.InterruptException e) {
                    LOGGER.info("Task {}, caught interrupt exception during reading the sync topic {}", consumerGroup, e);
                    throw new InterruptedException();
                }
                catch (Exception e) {
                    LOGGER.info("Task {}, Error during seek back the Sync Topic {}", consumerGroup, e);
                    errorHandler.accept(
                            new SpannerConnectorException("Error during seek back the Sync Topic", e));
                    return;
                }
            }

        }
        catch (Exception ex) {
            LOGGER.info("Shutdown consumer {} for ex {}", consumerGroup, ex);
            shutdownConsumer(consumer);
            throw ex;
        }

        thread = new Thread(
                () -> {
                    try {
                        long commitOffsetStart = System.currentTimeMillis();
                        LOGGER.info("Task {}, beginning to poll the sync topic", consumerGroup);
                        Stopwatch sw = Stopwatch.accumulating().start();
                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                final Duration totalDuration = sw.stop().durations().statistics().getTotal();
                                if (totalDuration.toMillis() >= pollInterval.toMillis()) {
                                    // Restart the stopwatch.
                                    LOGGER.info("Task {}, still polling the sync topic", consumerGroup);
                                    sw = Stopwatch.accumulating().start();
                                }
                                else {
                                    sw.start();
                                }
                                poll(consumer, endOffset);
                                if (!consumerFactory.isAutoCommitEnabled()
                                        && commitOffsetStart + commitOffsetsInterval < System.currentTimeMillis()) {

                                    consumer.commitSync(commitOffsetsTimeout);
                                    commitOffsetStart = System.currentTimeMillis();
                                }
                            }
                            catch (org.apache.kafka.common.errors.InterruptException
                                    | InterruptedException ex) {
                                LOGGER.error("TaskSyncEventListener, caught interrupt exception {}, {}", consumerGroup, ex);
                                return;
                            }
                            catch (Exception e) {
                                errorHandler.accept(
                                        new SpannerConnectorException("Error during poll from the Sync Topic", e));
                                return;
                            }
                        }
                    }
                    finally {
                        shutdownConsumer(consumer);
                    }
                },
                "SpannerConnector-TaskSyncEventListener");
        thread.setUncaughtExceptionHandler((t, ex) -> {
            LOGGER.error("Error in SpannerConnector-TaskSyncEventListener, task {}, ex {}", consumerGroup, ex.getStackTrace());
            errorHandler.accept(new RuntimeException(ex));
        });

        thread.start();

    }

    private int poll(Consumer<String, byte[]> consumer, long endOffset)
            throws InvalidProtocolBufferException, InterruptedException {

        ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);
        LOGGER.trace("listen: poll messages count: {}", records.count());

        if (records.isEmpty()) {
            return 0;
        }
        for (ConsumerRecord<String, byte[]> record : records) {

            TaskSyncEvent taskSyncEvent = parseSyncEvent(record);
            debug(LOGGER, "Receive SyncEvent from Kafka topic: {}", taskSyncEvent);

            if (record.offset() == endOffset - 1) {
                LOGGER.info("Task {}, can begin to initiate rebalancing", consumerGroup);
            }
            int i = 0;
            for (BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer : eventConsumers) {
                i++;
                boolean canInitiateRebalancing = (record.offset() >= endOffset - 1);
                eventConsumer.accept(
                        taskSyncEvent,
                        SyncEventMetadata.builder()
                                .offset(record.offset())
                                // Once we have consumed all the messages present in the sync topic at the
                                // start of the connector, we can then connect to the rebalance topic.
                                .canInitiateRebalancing(canInitiateRebalancing)
                                .build());
            }
        }
        return records.count();
    }

    private void seekBackToPreviousEpoch(
                                         Consumer<String, byte[]> consumer, TopicPartition topicPartition, long beginOffset, long currOffset)
            throws InvalidProtocolBufferException {
        if (!seekBackToPreviousEpoch) {
            LOGGER.info("Task {}, not seeking back to previous epoch");
            return;
        }
        ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);

        long currentOffset = currOffset;
        while (records.isEmpty()) {
            currentOffset = currentOffset - 1;
            if (currOffset - currentOffset >= 100) {
                throw new DebeziumException("Task " + consumerGroup + "failed to poll last message from the sync topic");
            }
            LOGGER.warn("Task {}, listen: fail to poll last message, trying again", consumerGroup);
            consumer.seek(topicPartition, currentOffset);
            records = consumer.poll(pollDuration);
        }

        ConsumerRecord<String, byte[]> lastRecord = records.iterator().next();
        TaskSyncEvent taskSyncEvent = parseSyncEvent(lastRecord);

        long previousEpochOffset = taskSyncEvent.getEpochOffset();
        long startOffset = Math.max(previousEpochOffset, beginOffset);
        LOGGER.info("Task {}, listen: found previous epoch offset {} and begin offset {}", consumerGroup, previousEpochOffset, beginOffset);

        LOGGER.info("Task {}, listen: seek back to previous epoch offset: {}", consumerGroup, startOffset);
        consumer.seek(topicPartition, startOffset);
    }

    private TaskSyncEvent parseSyncEvent(ConsumerRecord<String, byte[]> record)
            throws InvalidProtocolBufferException {
        return SyncEventFromProtoMapper.mapFromProto(
                SyncEventProtos.SyncEvent.parseFrom(record.value()));
    }

    private void shutdownConsumer(Consumer<String, byte[]> consumer) {
        try {
            LOGGER.info("TaskSyncEventListener, Shutting down consumer {}", consumerGroup);
            consumer.unsubscribe();
            consumer.close();
        }
        catch (org.apache.kafka.common.errors.InterruptException e) {
            if (!Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void shutdown() {
        if (thread == null) {
            return;
        }
        thread.interrupt();

        while (!thread.getState().equals(Thread.State.TERMINATED)) {
        }
        thread = null;
    }
}
