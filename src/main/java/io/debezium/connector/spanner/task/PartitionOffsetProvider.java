/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.connector.spanner.context.offset.PartitionOffset;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.OffsetReceivingTimeMetricEvent;

/**
 * Retrieves offsets from Kafka Connect
 * and publishes appropriate metrics
 */
public class PartitionOffsetProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionOffsetProvider.class);

    // Offset lookups are short-lived, I/O-bound calls against Kafka Connect's offset backing
    // store. A small fixed pool bounds how many threads can ever be pinned by a stuck
    // OffsetStorageReader call (see shutdown() javadoc), instead of the unbounded growth that
    // Executors.newCachedThreadPool() allows when future.cancel(true) fails to actually
    // interrupt a stuck call.
    private static final int EXECUTOR_POOL_SIZE = 2;

    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);

    private final OffsetStorageReader offsetStorageReader;
    private final MetricsEventPublisher metricsEventPublisher;
    private final long batchRetrievalTimeoutMs;

    private final ExecutorService executor;

    public PartitionOffsetProvider(OffsetStorageReader offsetStorageReader, MetricsEventPublisher metricsEventPublisher,
                                   long batchRetrievalTimeoutMs) {
        this.offsetStorageReader = offsetStorageReader;
        this.metricsEventPublisher = metricsEventPublisher;
        this.batchRetrievalTimeoutMs = batchRetrievalTimeoutMs;
        this.executor = Executors.newFixedThreadPool(EXECUTOR_POOL_SIZE, new PartitionOffsetProviderThreadFactory());
    }

    /**
     * Shuts down the offset-retrieval executor. Should be called once when the owning task is
     * torn down, after all consumers of this provider (LowWatermarkCalculationJob, PartitionFactory)
     * have already been stopped, so no in-flight callers race with the shutdown.
     *
     * <p>Uses the standard two-phase shutdown idiom: first ask in-flight tasks to finish naturally
     * ({@code shutdown()}), then forcibly interrupt anything still running ({@code shutdownNow()})
     * if it doesn't complete within a short grace period.
     */
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warn("PartitionOffsetProvider executor did not terminate cleanly within timeout, forcing shutdown");
                executor.shutdownNow();
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.warn("PartitionOffsetProvider executor did not terminate even after forced shutdown");
                }
                else {
                    LOGGER.info("PartitionOffsetProvider executor forcibly shut down");
                }
            }
            else {
                LOGGER.info("PartitionOffsetProvider executor shut down cleanly");
            }
        }
        catch (InterruptedException e) {
            LOGGER.warn("Interrupted while waiting for PartitionOffsetProvider executor to terminate, forcing shutdown", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static class PartitionOffsetProviderThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "SpannerConnector-PartitionOffsetProvider-" + THREAD_COUNTER.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }

    public Timestamp getOffset(PartitionState token) {
        Map<String, String> spannerPartition = new SpannerPartition(token.getToken()).getSourcePartition();

        Map<String, ?> result = retrieveOffsetMap(spannerPartition);
        if (result == null) {
            LOGGER.warn("Token {} no stored offset found", token);
            return null;
        }
        LOGGER.info("Successfully retrieved offset {} for token {}", result, token);
        return PartitionOffset.extractOffset(result);
    }

    public Map<String, Timestamp> getOffsets(Collection<String> partitions) {
        Instant startTime = Instant.now();

        List<Map<String, String>> partitionsMapList = partitions.stream()
                .map(token -> new SpannerPartition(token).getSourcePartition())
                .collect(Collectors.toList());

        Map<Map<String, String>, Map<String, Object>> result;
        Future<Map<Map<String, String>, Map<String, Object>>> future = executor.submit(
                () -> this.offsetStorageReader.offsets(partitionsMapList));
        try {
            result = future.get(batchRetrievalTimeoutMs, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex) {
            LOGGER.error("Failed to retrieve batch offsets for {} partitions in time", partitions.size(), ex);
            future.cancel(true);
            return Map.of();
        }
        catch (InterruptedException e) {
            LOGGER.error("Interrupted while retrieving batch offsets for {} partitions", partitions.size(), e);
            future.cancel(true);
            Thread.currentThread().interrupt();
            return Map.of();
        }
        catch (ExecutionException e) {
            LOGGER.error("Failed to retrieve batch offsets for {} partitions: {}", partitions.size(), e.toString(), e);
            future.cancel(true);
            return Map.of();
        }

        if (result == null) {
            return Map.of();
        }

        metricsEventPublisher.publishMetricEvent(OffsetReceivingTimeMetricEvent.from(startTime));

        Map<String, Timestamp> map = new HashMap<>();

        for (Map.Entry<Map<String, String>, Map<String, Object>> entry : result.entrySet()) {
            map.put(SpannerPartition.extractToken(entry.getKey()),
                    PartitionOffset.extractOffset(entry.getValue()));
        }

        return map;
    }

    private Map<String, ?> retrieveOffsetMap(Map<String, String> spannerPartition) {
        Instant startTime = Instant.now();
        Map<String, ?> result = null;
        Future<Map<String, ?>> future = executor.submit(new ExecutorServiceCallable(offsetStorageReader, spannerPartition));
        try {
            result = future.get(5, TimeUnit.SECONDS);
        }
        catch (TimeoutException ex) {
            // handle the timeout
            LOGGER.error("Token {}, failed to retrieve offset in time", spannerPartition, ex);
        }
        catch (InterruptedException e) {
            // handle the interrupts
            LOGGER.error("Token {},interrupting PartitionOffsetProvider", spannerPartition, e);
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e) {
            // handle other exceptions
            LOGGER.error("Token {}, failed to retrieve offset {}:{}", spannerPartition, e.toString(), e.getStackTrace());
        }
        finally {
            future.cancel(true); // may or may not desire this
        }
        metricsEventPublisher.publishMetricEvent(OffsetReceivingTimeMetricEvent.from(startTime));
        return result;
    }

    public static class ExecutorServiceCallable implements Callable<Map<String, ?>> {

        private OffsetStorageReader offsetStorageReader;
        private Map<String, String> spannerPartition;

        public ExecutorServiceCallable(OffsetStorageReader offsetStorageReader, Map<String, String> spannerPartition) {
            this.offsetStorageReader = offsetStorageReader;
            this.spannerPartition = spannerPartition;
        }

        @Override
        public Map<String, ?> call() throws Exception {
            try {
                return this.offsetStorageReader.offset(spannerPartition);
            }
            catch (Exception e) {
                LOGGER.error("Offsetstoragereader throwing exception", e);
                throw e;
            }
        }
    }

}
