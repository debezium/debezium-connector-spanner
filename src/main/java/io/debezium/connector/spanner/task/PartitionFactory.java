/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.Partition;
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

    public PartitionFactory(PartitionOffsetProvider partitionOffsetProvider, MetricsEventPublisher metricsEventPublisher) {
        this.partitionOffsetProvider = partitionOffsetProvider;
        this.metricsEventPublisher = metricsEventPublisher;
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

    public Partition getPartition(PartitionState partitionState) {
        LOGGER.info("Partition factory, getting partition {}", partitionState);
        return Partition.builder()
                .token(partitionState.getToken())
                .startTimestamp(getOffset(partitionState))
                .endTimestamp(partitionState.getEndTimestamp())
                .parentTokens(partitionState.getParents())
                .build();
    }

    private Timestamp getOffset(PartitionState partitionState) {

        LOGGER.info("Partition factory, getting offset {}", partitionState);
        final Timestamp offset = partitionOffsetProvider.getOffset(partitionState);
        LOGGER.info("Partition factory, got partition {}", partitionState);

        Timestamp startTime;

        if (offset != null) {

            if (offset.toSqlTimestamp().before(partitionState.getStartTimestamp().toSqlTimestamp())) {
                Map<String, String> offsetMap = partitionOffsetProvider.getOffsetMap(partitionState);

                LOGGER.warn("Incorrect offset, start time will be taken for partition {}, offsetMap {}", partitionState.getToken(), offsetMap);

                startTime = partitionState.getStartTimestamp();
            }
            else {
                LOGGER.info("Found previous offset {}", Map.of(partitionState.getToken(), offset.toString()));

                startTime = offset;
            }
        }
        else {
            LOGGER.info("Previous offset not found, start time will be taken {}",
                    Map.of(partitionState.getToken(), partitionState.getStartTimestamp()));

            startTime = partitionState.getStartTimestamp();
        }

        metricsEventPublisher.publishMetricEvent(PartitionOffsetLagMetricEvent.from(partitionState.getToken(), startTime));

        return startTime;
    }
}
