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

            while (true) {

                Set<String> pendingToFinish = finishingPartitionManager.getPendingFinishPartitions();

                pendingToFinish.forEach(token -> partition.computeIfAbsent(token, token1 -> Instant.now()));

                partition.keySet().stream().filter(token -> !pendingToFinish.contains(token)).forEach(partition::remove);

                List<String> tokens = new ArrayList<>();

                partition.forEach((token, instant) -> {
                    Instant now = Instant.now();
                    LOGGER.info("Partitions awaiting finish: {}", tokens);
                    if (now.isAfter(instant.plus(timeout))) {
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
        this.thread.interrupt();
    }
}
