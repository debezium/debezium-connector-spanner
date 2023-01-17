/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinishPartitionWatchDog {

    private static final Logger LOGGER = LoggerFactory.getLogger(FinishPartitionWatchDog.class);
    private final Thread thread;

    private final Map<String, Instant> partition = new HashMap<>();

    public FinishPartitionWatchDog(FinishingPartitionManager finishingPartitionManager, Duration timeout, Consumer<List<String>> errorHandler) {

        this.thread = new Thread(() -> {

            Instant lastUpdatedTime = Instant.now();
            while (true) {

                Set<String> pendingToFinish = finishingPartitionManager.getPendingFinishPartitions();
                Set<String> pending = finishingPartitionManager.getPendingPartitions();

                pendingToFinish.forEach(
                        token -> partition.computeIfAbsent(token, token1 -> Instant.now()));

                if (Instant.now().isAfter(lastUpdatedTime.plus(Duration.ofSeconds(600)))) {
                    LOGGER.info("Get pending partitions: {}", pendingToFinish);
                    LOGGER.info("Get pending total partitions: {}", pending);
                    lastUpdatedTime = Instant.now();
                }

                Iterator<Map.Entry<String, Instant>> itr = partition.entrySet().iterator();
                while (itr.hasNext()) {
                    Map.Entry<String, Instant> entry = itr.next();
                    if (!pendingToFinish.contains(entry.getKey())) {
                        itr.remove();
                    }
                }

                List<String> tokens = new ArrayList<>();

                Instant currentTime = Instant.now();
                partition.forEach(
                        (token, instant) -> {
                            if (currentTime.isAfter(instant.plus(timeout))) {
                                tokens.add(token);
                            }
                        });

                if (!tokens.isEmpty()) {
                    LOGGER.warn("Partitions awaiting finish : {}, timeout: {}", tokens, timeout);
                    errorHandler.accept(tokens);
                }

                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        }, "SpannerConnector-FinishingPartitionWatchDog");
        this.thread.start();
    }

    public void stop() {
        LOGGER.info("Interrupting SpannerConnector-FinishingPartitionWatchDog");
        this.thread.interrupt();
    }
}
