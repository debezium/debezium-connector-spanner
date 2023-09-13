/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import java.util.List;

import io.debezium.connector.spanner.PartitionManager;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.task.state.NewPartitionsEvent;
import io.debezium.connector.spanner.task.state.PartitionStatusUpdateEvent;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;
import io.debezium.function.BlockingConsumer;

/**
 * This class produces events depending on the type of record received from the change
 * stream (i.e. data change, heartbeat, child partitions record), and sends them to
 * TaskStateChangeEventHandler.
 * This class sends the following events to TaskStateChangeEventHandler: (1) Sharing / owning new
 * child partition tokens (2) setting the last commit timestamp for a partition (3) updating
 * the change stream partition state to RUNNING / FINISHED
 */
public class SynchronizedPartitionManager implements PartitionManager {

    private final BlockingConsumer<TaskStateChangeEvent> syncEventPublisher;

    public SynchronizedPartitionManager(BlockingConsumer<TaskStateChangeEvent> syncEventPublisher) {
        this.syncEventPublisher = syncEventPublisher;
    }

    @Override
    public void newChildPartitions(List<Partition> partitions) throws InterruptedException {

        syncEventPublisher.accept(new NewPartitionsEvent(partitions));
    }

    @Override
    public void updateToFinished(String token) throws InterruptedException {
        LOGGER.info("Token {}, updating to finished");

        syncEventPublisher.accept(new PartitionStatusUpdateEvent(token, PartitionStateEnum.FINISHED));
    }

    @Override
    public void updateToRunning(String token) throws InterruptedException {

        syncEventPublisher.accept(new PartitionStatusUpdateEvent(token, PartitionStateEnum.RUNNING));
    }

    @Override
    public void updateToReadyForStreaming(String token) throws InterruptedException {
        syncEventPublisher.accept(new PartitionStatusUpdateEvent(token, PartitionStateEnum.READY_FOR_STREAMING));
    }

}
