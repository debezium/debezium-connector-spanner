/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.PartitionKey;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.PartitionOffsetLagMetricEvent;

/**
 * Creates {@link Partition} from {@link PartitionState},
 * retrieves offset for it
 */
public class PartitionFactory {

    private static final Logger LOGGER = getLogger(PartitionFactory.class);

    private final PartitionOffsetProvider partitionOffsetProvider;

    private final MetricsEventPublisher metricsEventPublisher;

    private final List<String> placementTvfNames;

    public PartitionFactory(PartitionOffsetProvider partitionOffsetProvider, MetricsEventPublisher metricsEventPublisher) {
        this(partitionOffsetProvider, metricsEventPublisher, Collections.emptyList());
    }

    public PartitionFactory(PartitionOffsetProvider partitionOffsetProvider, MetricsEventPublisher metricsEventPublisher,
                            List<String> placementTvfNames) {
        this.partitionOffsetProvider = partitionOffsetProvider;
        this.metricsEventPublisher = metricsEventPublisher;
        this.placementTvfNames = placementTvfNames == null ? Collections.emptyList() : placementTvfNames;
    }

    public Partition initPartition(Timestamp startTime, Timestamp endTime) {
        Partition partition = Partition.builder()
                .token(InitialPartition.PARTITION_TOKEN)
                .parentTokens(Set.of())
                .startTimestamp(startTime)
                .endTimestamp(endTime)
                .build();

        metricsEventPublisher.publishMetricEvent(PartitionOffsetLagMetricEvent.from(partition.getToken(), startTime));

        return partition;
    }

    /**
     * Creates the root partition(s) to start streaming from. When the change stream is configured
     * with {@code gcp.spanner.placement.tvf.names}, each placement TVF has its own independent
     * partition token space on the Spanner side, so one root partition per configured TVF name is
     * created here (each identified by the raw root token and its TVF name), and
     * every partition discovered afterwards inherits its parent's {@code tvfName} (see
     * {@link Partition#getTvfName()}). Otherwise, a single root partition is created, matching the
     * default (non per-placement-TVF) behavior.
     */
    public List<Partition> initPartitions(Timestamp startTime, Timestamp endTime) {
        if (placementTvfNames.isEmpty()) {
            return List.of(initPartition(startTime, endTime));
        }

        List<Partition> partitions = new ArrayList<>();
        for (String tvfName : placementTvfNames) {
            Partition partition = Partition.builder()
                    .token(InitialPartition.PARTITION_TOKEN)
                    .parentTokens(Set.of())
                    .startTimestamp(startTime)
                    .endTimestamp(endTime)
                    .tvfName(tvfName)
                    .build();

            metricsEventPublisher.publishMetricEvent(PartitionOffsetLagMetricEvent.from(partition.getToken(), startTime));
            partitions.add(partition);
        }
        return partitions;
    }

    public Map<PartitionKey, Partition> getPartitions(List<PartitionState> partitionStates) {
        Map<PartitionKey, Timestamp> offsets = partitionOffsetProvider.getOffsets(partitionStates);

        Map<PartitionKey, Partition> partitionMap = new HashMap<>();
        for (PartitionState partitionState : partitionStates) {
            Timestamp offset = offsets.get(partitionState.getKey());
            Timestamp startTime = resolveOffset(partitionState, offset);

            partitionMap.put(partitionState.getKey(), Partition.builder()
                    .token(partitionState.getToken())
                    .startTimestamp(startTime)
                    .endTimestamp(partitionState.getEndTimestamp())
                    .parentTokens(partitionState.getParents())
                    .lastBoundaryRecordSequence(resolveLastBoundaryRecordSequence(partitionState))
                    .tvfName(partitionState.getTvfName())
                    .build());
        }
        return partitionMap;
    }

    public Partition getPartition(PartitionState partitionState) {
        Timestamp offset = partitionOffsetProvider.getOffset(partitionState);
        return Partition.builder()
                .token(partitionState.getToken())
                .startTimestamp(resolveOffset(partitionState, offset))
                .endTimestamp(partitionState.getEndTimestamp())
                .parentTokens(partitionState.getParents())
                .lastBoundaryRecordSequence(resolveLastBoundaryRecordSequence(partitionState))
                .tvfName(partitionState.getTvfName())
                .build();
    }

    private String resolveLastBoundaryRecordSequence(PartitionState partitionState) {
        if (partitionState.getLastBoundaryRecordSequence() != null) {
            return partitionState.getLastBoundaryRecordSequence();
        }
        if (partitionState.getMoveInState() != null) {
            return partitionState.getMoveInState().getRecordSequence();
        }
        return null;
    }

    private Timestamp resolveOffset(PartitionState partitionState, Timestamp offset) {
        Timestamp startTimestamp = partitionState.getStartTimestamp();
        Timestamp processedTimestamp = partitionState.getProcessedTimestamp();

        if (offset != null && offset.compareTo(startTimestamp) < 0) {
            LOGGER.warn("Incorrect offset {}, ignoring for partition {}", offset, partitionState.getToken());
            offset = null;
        }

        Timestamp startTime;
        if (offset != null) {
            startTime = offset;
            LOGGER.info("Resuming partition {} from committed offset {} (processedTimestamp={})",
                    partitionState.getToken(), offset, processedTimestamp);
        }
        else if (processedTimestamp != null && processedTimestamp.compareTo(startTimestamp) > 0) {
            LOGGER.info("Resuming partition {} from processedTimestamp {} (no committed offset found)",
                    partitionState.getToken(), processedTimestamp);
            startTime = processedTimestamp;
        }
        else {
            LOGGER.info("No previous offset found, using startTimestamp {} for partition {}",
                    startTimestamp, partitionState.getToken());
            startTime = startTimestamp;
        }

        if (partitionState.getMoveInState() != null) {
            Timestamp moveInTimestamp = partitionState.getMoveInState().getTimestamp();
            if (startTime.compareTo(moveInTimestamp) < 0) {
                LOGGER.info("Partition {} has MoveInState at {}, adjusting startTime from {} to moveInTimestamp",
                        partitionState.getToken(), moveInTimestamp, startTime);
                startTime = moveInTimestamp;
            }
        }

        metricsEventPublisher.publishMetricEvent(PartitionOffsetLagMetricEvent.from(partitionState.getToken(), startTime));
        return startTime;
    }
}
