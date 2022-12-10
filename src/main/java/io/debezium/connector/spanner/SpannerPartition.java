/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.Map;
import java.util.Objects;

import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

/**
 * Describes the Spanner source partition
 */
public class SpannerPartition implements Partition {

    private static final String PARTITION_TOKEN_KEY = "partitionToken";

    private final String partitionToken;

    public SpannerPartition(String partitionToken) {
        this.partitionToken = partitionToken;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(PARTITION_TOKEN_KEY, partitionToken);
    }

    public String toString() {
        return "SpannerPartition[" + getSourcePartition() + "]";
    }

    public String getValue() {
        return partitionToken;
    }

    public static String extractToken(Map<String, ?> sourcePartition) {
        return (String) sourcePartition.get(SpannerPartition.PARTITION_TOKEN_KEY);
    }

    public static SpannerPartition getInitialSpannerPartition() {
        return new SpannerPartition(InitialPartition.PARTITION_TOKEN);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SpannerPartition that = (SpannerPartition) o;
        return Objects.equals(partitionToken, that.partitionToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionToken);
    }
}
