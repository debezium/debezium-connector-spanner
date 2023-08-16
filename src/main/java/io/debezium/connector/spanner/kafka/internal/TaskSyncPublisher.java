/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static io.debezium.connector.spanner.task.LoggerUtils.debug;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

import io.debezium.connector.spanner.exception.SpannerConnectorException;
import io.debezium.connector.spanner.kafka.event.proto.SyncEventProtos;
import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.kafka.internal.proto.SyncEventToProtoMapper;

/**
 * Sends Sync Events with task internal state updates to Kafka Sync topic
 */
public class TaskSyncPublisher {
    private static final Logger LOGGER = getLogger(TaskSyncPublisher.class);

    private final String topic;
    private final KafkaProducer<String, byte[]> producer;
    private volatile Instant lastTime;
    private final BufferedPublisher bufferedPublisher;
    private final Consumer<RuntimeException> errorHandler;

    private final String taskUid;

    public TaskSyncPublisher(String taskUid, String topic, int syncEventPublisherWaitingTimeout, ProducerFactory<String, byte[]> producerFactory,
                             Consumer<RuntimeException> errorHandler) {
        this.topic = topic;
        this.producer = producerFactory.createProducer();
        this.errorHandler = errorHandler;
        this.taskUid = taskUid;

        if (syncEventPublisherWaitingTimeout > 0) {
            this.bufferedPublisher = new BufferedPublisher(
                    this.taskUid,
                    "Buffer-Pub",
                    syncEventPublisherWaitingTimeout,
                    this::publishImmediately,
                    this::publishSyncEvent);
            this.bufferedPublisher.start();
        }
        else {
            this.bufferedPublisher = null;
        }
    }

    public void send(TaskSyncEvent taskSyncEvent) {
        if (bufferedPublisher == null) {
            publishSyncEvent(taskSyncEvent);
        }
        else {
            LOGGER.debug("Buffering Sync Event, type: {}, timestamp: {}", taskSyncEvent.getMessageType(), taskSyncEvent.getMessageTimestamp());
            bufferedPublisher.buffer(taskSyncEvent);
        }
    }

    private void publishSyncEvent(TaskSyncEvent taskSyncEvent) {
        debug(LOGGER, "Send SyncEvent to Kafka topic, type: {}, timestamp: {}, event: {}", taskSyncEvent.getMessageType(), taskSyncEvent.getMessageTimestamp(),
                taskSyncEvent);

        SyncEventProtos.SyncEvent protoEvent = SyncEventToProtoMapper.mapToProto(taskSyncEvent);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, taskSyncEvent.getTaskUid(), protoEvent.toByteArray());
        try {
            Instant sendTime = Instant.now();
            producer.send(record).get();
            producer.flush();
            if (Instant.now().isAfter(sendTime.plus(Duration.ofSeconds(60)))) {
                long seconds = Instant.now().getEpochSecond() - sendTime.getEpochSecond();
                LOGGER.info(
                        "Task Uid {} published record {} with {} seconds latency",
                        this.taskUid,
                        record,
                        seconds);
            }

            lastTime = Instant.now();
        }
        catch (ExecutionException e) {
            errorHandler.accept(new SpannerConnectorException("Error during publishing to the Sync Topic", e));
        }
        catch (InterruptedException e) {
        }
    }

    public void close() {
        if (bufferedPublisher != null) {
            bufferedPublisher.close();
        }
        producer.close();
    }

    public Instant getLastTime() {
        return lastTime;
    }

    private boolean publishImmediately(TaskSyncEvent syncEvent) {
        return !(syncEvent.getMessageType() == null || syncEvent.getMessageType() == MessageTypeEnum.REGULAR);
    }
}
