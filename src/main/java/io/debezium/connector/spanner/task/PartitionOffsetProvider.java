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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    private final OffsetStorageReader offsetStorageReader;
    private final MetricsEventPublisher metricsEventPublisher;

    private final ExecutorService executor;

    public PartitionOffsetProvider(OffsetStorageReader offsetStorageReader, MetricsEventPublisher metricsEventPublisher) {
        this.offsetStorageReader = offsetStorageReader;
        this.metricsEventPublisher = metricsEventPublisher;
        this.executor = Executors.newCachedThreadPool();
    }

    public Timestamp getOffset(PartitionState token) {
        Instant startTime = Instant.now();
        LOGGER.info("Token {}, Trying to get offset", token);
        Map<String, String> spannerPartition = new SpannerPartition(token.getToken()).getSourcePartition();
        LOGGER.info("Token {}, Retrieved spanner partition", token);

        Map<String, ?> result = null;
        LOGGER.info("Token {}, Submitting executor task", token);
        Future<Map<String, ?>> future = executor.submit(new ExecutorServiceCallable(offsetStorageReader, spannerPartition));
        try {
            LOGGER.info("Token {}, Retrieving future", token);
            result = future.get(5, TimeUnit.SECONDS);
        }
        catch (TimeoutException ex) {
            // handle the timeout
            LOGGER.error("Token {}, failed to retrieve offset timely {}", token, ex);
        }
        catch (InterruptedException e) {
            // handle the interrupts
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e) {
            // handle other exceptions
            LOGGER.error("Token {}, failed to retrieve offset {}", token, e);
        }
        finally {
            future.cancel(true); // may or may not desire this
        }

        if (result == null) {
            LOGGER.error("Token {}, failed to retrieve offset, returning start timestamp", token);
            return token.getStartTimestamp();
        }

        metricsEventPublisher.publishMetricEvent(OffsetReceivingTimeMetricEvent.from(startTime));

        return PartitionOffset.extractOffset(result);
    }

    public Map<String, String> getOffsetMap(PartitionState token) {

        LOGGER.info("Token {}, Trying to get offset map", token);
        Map<String, String> spannerPartition = new SpannerPartition(token.getToken()).getSourcePartition();
        Map<String, ?> result = this.offsetStorageReader.offset(spannerPartition);
        LOGGER.info("Token {},  Got offset map", token);

        if (result == null) {
            return Map.of();
        }
        return (Map<String, String>) result;
    }

    public Map<String, Timestamp> getOffsets(Collection<String> partitions) {
        Instant startTime = Instant.now();

        List<Map<String, String>> partitionsMapList = partitions.stream()
                .map(token -> new SpannerPartition(token).getSourcePartition())
                .collect(Collectors.toList());

        // LOGGER.info("Token {}, Trying to get offsets", partitions);
        Map<Map<String, String>, Map<String, Object>> result = this.offsetStorageReader.offsets(partitionsMapList);

        // LOGGER.info("Token {}, Got offsets", partitions);

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

    public static class ExecutorServiceCallable implements Callable<Map<String, ?>> {

        private OffsetStorageReader offsetStorageReader;
        private Map<String, String> spannerPartition;

        public ExecutorServiceCallable(OffsetStorageReader offsetStorageReader, Map<String, String> spannerPartition) {
            offsetStorageReader = offsetStorageReader;
            spannerPartition = spannerPartition;
        }

        @Override
        public Map<String, ?> call() throws Exception {
            return this.offsetStorageReader.offset(spannerPartition);
        }
    }

}
