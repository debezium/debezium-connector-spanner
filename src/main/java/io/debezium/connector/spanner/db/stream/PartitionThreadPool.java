/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Partition thread pool, a thread is created for each partition token
 */
public class PartitionThreadPool {

    private final Map<String, Thread> threadMap = new HashMap<>();

    public synchronized boolean submit(String token, Runnable runnable) {

        if (threadMap.containsKey(token)) {
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
            thread.interrupt();
        }
        threadMap.remove(token);
    }

    public synchronized void shutdown() {
        clean();
        threadMap.values().forEach(Thread::interrupt);
        while (!threadMap.values().stream().allMatch(thread -> thread.getState().equals(Thread.State.TERMINATED))) {
        }
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
