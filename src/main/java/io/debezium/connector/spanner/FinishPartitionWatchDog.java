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

import io.debezium.util.Clock;
import io.debezium.util.Metronome;

public class FinishPartitionWatchDog {

    private static final Logger LOGGER = LoggerFactory.getLogger(FinishPartitionWatchDog.class);
    private volatile Thread thread;
    private final Map<String, Instant> partition = new HashMap<>();
    private final Duration pollInterval = Duration.ofMillis(60000);
    private final Duration sleepInterval = Duration.ofMillis(100);
    private final Clock clock;

    public FinishPartitionWatchDog(FinishingPartitionManager finishingPartitionManager, Duration timeout, Consumer<List<String>> errorHandler) {
        this.clock = Clock.system();

        this.thread = new Thread(() -> {

            final Metronome metronome = Metronome.sleeper(pollInterval, clock);

            Instant lastUpdatedTime = Instant.now();
            while (!Thread.currentThread().isInterrupted()) {

                Set<String> pendingToFinish = finishingPartitionManager.getPendingFinishPartitions();
                Set<String> pending = finishingPartitionManager.getPendingPartitions();

                pendingToFinish.forEach(
                        token -> partition.computeIfAbsent(token, token1 -> Instant.now()));

                if (Instant.now().isAfter(lastUpdatedTime.plus(Duration.ofSeconds(600)))) {
                    LOGGER.info("Get pending to finish partitions: {}", pendingToFinish);
                    LOGGER.info("Get pending partitions: {}", pending);
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
                    // Sleep for pollInterval.
                    metronome.pause();
                }
                catch (InterruptedException e) {
                    LOGGER.info("Interrupting SpannerConnector-FinishingPartitionWatchDog");
                    Thread.currentThread().interrupt();
                    return;
                }
            }

        }, "SpannerConnector-FinishingPartitionWatchDog");
        this.thread.start();
    }

    public void stop() {
        LOGGER.info("Interrupting SpannerConnector-FinishingPartitionWatchDog");
        this.thread.interrupt();
        final Metronome metronome = Metronome.sleeper(sleepInterval, clock);
        while (!thread.getState().equals(Thread.State.TERMINATED)) {
            try {
                // Sleep for sleepInterval.
                metronome.pause();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        this.thread = null;
    }
}
