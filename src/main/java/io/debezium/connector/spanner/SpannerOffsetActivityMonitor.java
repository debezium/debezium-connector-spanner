/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.context.offset.PartitionOffset;
import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset commit timestamp of the change stream partition being streamed is compared
 * against the value captured when that partition was last consulted, and when the timestamp
 * has not moved, a warning is logged. Spanner emits heartbeat records for every change
 * stream partition even when the captured tables are quiet, so a stationary commit timestamp
 * means the change stream is no longer delivering records for the partition rather than that
 * there are no changes.
 * <p>
 * Only the most recently consulted partition is tracked. The Spanner partition set is
 * dynamic, partitions split, merge, and finish continuously, so tracking every token would
 * grow without bound over the connector's lifetime; comparing consecutive observations of
 * the same token keeps the state bounded while still detecting a stalled partition.
 *
 * @author Chris Cranford
 */
public class SpannerOffsetActivityMonitor implements OffsetActivityMonitor<SpannerPartition, SpannerOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerOffsetActivityMonitor.class);

    private final Duration checkInterval;

    private SpannerPartition previousPartition;
    private Timestamp previousOffset;

    public SpannerOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public void checkForStaleOffsets(SpannerPartition partition, SpannerOffsetContext offsetContext) {
        final Timestamp offset = PartitionOffset.extractOffset(offsetContext.getOffset());

        // Check for stale state
        if (offset != null
                && Objects.equals(previousPartition, partition)
                && Objects.equals(previousOffset, offset)) {
            LOGGER.warn("Offset commit timestamp {} for partition token {} has not changed in {} milliseconds. " +
                    "Spanner emits heartbeat records for every change stream partition even when idle, so this " +
                    "may indicate the change stream is no longer delivering records for this partition.",
                    offset, partition.getValue(), checkInterval.toMillis());
        }

        // Update tracked stats
        previousPartition = partition;
        previousOffset = offset;
    }

}