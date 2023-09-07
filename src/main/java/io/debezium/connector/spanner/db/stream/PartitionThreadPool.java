/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Partition thread pool, a thread is created for each partition token
 */
public class PartitionThreadPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionThreadPool.class);

    private final Map<String, Thread> threadMap = new HashMap<>();

    private final Duration sleepInterval = Duration.ofMillis(100);

    private final Clock clock = Clock.system();

    public synchronized boolean submit(String token, Runnable runnable) {

        if (threadMap.containsKey(token)) {
            LOGGER.info("Failed to submit token {} due to it existing in thread map {}", token,
                    threadMap.keySet().stream().collect(Collectors.toList()));
            return false;
        }

        clean();

        Thread thread = new Thread(runnable, "SpannerConnector-PartitionThreadPool");

        threadMap.put(token, thread);

        thread.start();

        return true;
    }

    public synchronized void stop(String token) {
        clean();
        Thread thread = threadMap.get(token);
        if (thread != null) {
            LOGGER.info("Interrupting SpannerConnector-PartitionThreadPool");
            thread.interrupt();
        }
        threadMap.remove(token);
    }

    public synchronized void shutdown(String taskUid) {
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
            Map<String, Thread> unterminatedThreads = threadMap.entrySet().stream().filter(entry -> !entry.getValue().getState().equals(Thread.State.TERMINATED))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            LOGGER.info("Task {}, Found {} unterminated threads {}, task {}", taskUid, unterminatedThreads.size(), unterminatedThreads);
            for (Map.Entry<String, Thread> unterminatedThread : unterminatedThreads.entrySet()) {
                LOGGER.info("Task {}, trying to terminate the token {} with state {}", taskUid, unterminatedThread.getKey(), unterminatedThread.getValue().getState());
                unterminatedThread.getValue().interrupt();
            }
            LOGGER.info("Finished trying to interrupt threads, cleaning {}", taskUid);
            clean();
        }
        LOGGER.info("Successfully shut down partition thread poll for task {}", taskUid);
    }

    private synchronized void clean() {
        Set<String> tokens = threadMap.entrySet().stream()
                .filter(entry -> entry.getValue().getState().equals(Thread.State.TERMINATED))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        tokens.forEach(threadMap::remove);
    }

    public synchronized Set<String> getActiveThreads() {
        clean();
        return Set.copyOf(threadMap.keySet());
    }

}
