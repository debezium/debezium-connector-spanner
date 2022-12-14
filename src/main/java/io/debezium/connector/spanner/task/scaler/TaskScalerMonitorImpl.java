/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.scaler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import io.debezium.connector.spanner.kafka.internal.TaskSyncEventListener;
import io.debezium.connector.spanner.kafka.internal.model.MessageTypeEnum;

/**
 * This class returns the initial task count upon startup. On each "UPDATE_EPOCH" Sync Event message received,
 * it checks whether or not we need to scale up / down the number of tasks.
 */
public class TaskScalerMonitorImpl implements TaskScalerMonitor {
    private final TaskSyncEventListener taskSyncEventListener;
    private final TaskScaler taskScaler;
    private final CountDownLatch initialLatch;
    private final AtomicInteger requiredTasksCount;
    private final int minTasks;

    public TaskScalerMonitorImpl(TaskSyncEventListener taskSyncEventListener,
                                 TaskScaler taskScaler, int minTasks) {
        this.taskSyncEventListener = taskSyncEventListener;
        this.taskScaler = taskScaler;
        this.requiredTasksCount = new AtomicInteger();
        this.initialLatch = new CountDownLatch(1);
        this.minTasks = minTasks;
    }

    public int start() throws InterruptedException {
        taskSyncEventListener.subscribe(((taskSyncEvent, ready) -> {

            // on first message, we need to get initial tasks count
            if (initialLatch.getCount() > 0) {
                requiredTasksCount.set(taskScaler.getTasksCount(taskSyncEvent, minTasks));
                initialLatch.countDown();

            }
            else {
                if (taskSyncEvent.getMessageType() == MessageTypeEnum.UPDATE_EPOCH) {
                    requiredTasksCount.set(taskScaler.ensureTasksScale(taskSyncEvent));
                }
            }
        }));

        taskSyncEventListener.start();

        // blocks the thread until get initial tasks count will be ready
        initialLatch.await();

        return requiredTasksCount.get();
    }

    public int getRequiredTasksCount() {
        return requiredTasksCount.get();
    }

    public void shutdown() {
        taskSyncEventListener.shutdown();
    }
}
