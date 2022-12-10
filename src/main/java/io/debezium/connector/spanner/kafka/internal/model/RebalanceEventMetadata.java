/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

import java.util.Objects;

/**
 * DTO for transferring Rebalance Event information,
 * between application layers.
 */
public class RebalanceEventMetadata {
    private final String consumerId;
    private final long rebalanceGenerationId;

    private final boolean leader;

    public RebalanceEventMetadata(String consumerId, long rebalanceGenerationId, boolean leader) {
        this.consumerId = consumerId;
        this.rebalanceGenerationId = rebalanceGenerationId;
        this.leader = leader;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public long getRebalanceGenerationId() {
        return rebalanceGenerationId;
    }

    public boolean isLeader() {
        return leader;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RebalanceEventMetadata that = (RebalanceEventMetadata) o;
        return rebalanceGenerationId == that.rebalanceGenerationId && Objects.equals(consumerId, that.consumerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerId, rebalanceGenerationId);
    }

    @Override
    public String toString() {
        return "RebalanceEventMetadata{" +
                "consumerId='" + consumerId + '\'' +
                ", rebalanceGenerationId=" + rebalanceGenerationId +
                ", leader=" + leader +
                '}';
    }
}
