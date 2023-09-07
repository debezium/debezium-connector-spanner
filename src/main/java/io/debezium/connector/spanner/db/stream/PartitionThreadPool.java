/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Partition thread pool, a thread is created for each partition token
 */
public class PartitionThreadPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionThreadPool.class);

    private final ConcurrentHashMap<String, Thread> threadMap = new ConcurrentHashMap<>();

    private final Duration sleepInterval = Duration.ofMillis(100);

    private final Clock clock = Clock.system();

    public boolean submit(String token, Runnable runnable) {
        clean();

        if (threadMap.containsKey(token)) {
            LOGGER.info("Fail to submit token in PartitionThreadPool {} since it is already contained in the map", token);
            return false;
        }

        AtomicBoolean insertedThread = new AtomicBoolean(false);

        threadMap.computeIfAbsent(token, k -> {
            Thread thread = new Thread(runnable, "SpannerConnector-PartitionThreadPool");
            thread.start();
            insertedThread.set(true);
            return thread;
        });
        if (!insertedThread.get()) {
            LOGGER.info("Fail to submit token in PartitionThreadPool {}", token);
        }

        return insertedThread.get();
    }

    public void stop(String token) {
        Thread thread = threadMap.remove(token);
        if (thread != null) {
            LOGGER.info("Interrupting SpannerConnector-PartitionThreadPool");
            thread.interrupt();
        }
    }

    public void shutdown(String taskUid) {
        LOGGER.info("Trying to shut down partition thread pool for task {}", taskUid);
        clean();
        threadMap.values().forEach(Thread::interrupt);
        final Metronome metronome = Metronome.sleeper(sleepInterval, clock);

        while (!threadMap.isEmpty() && !threadMap.values().stream().allMatch(thread -> thread.getState().equals(Thread.State.TERMINATED))) {
            try {
                // Sleep for sleepInterval.
                metronome.pause();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            LOGGER.info("Beginning to terminate threads, task {}", taskUid);
        }
        LOGGER.info("Successfully shut down partition thread poll for task {}", taskUid);
    }

    private void clean() {
        threadMap.entrySet().removeIf(entry -> entry.getValue().getState().equals(Thread.State.TERMINATED));
    }

    public Set<String> getActiveThreads() {
        clean();
        return Set.copyOf(threadMap.keySet());
    }

}
