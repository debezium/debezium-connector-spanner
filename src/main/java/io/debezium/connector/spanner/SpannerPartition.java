/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.PartitionKey;
import io.debezium.pipeline.spi.Partition;

/**
 * Describes the Spanner source partition. Per-placement TVF change streams can contain the
 * same raw partition token in different TVFs, so the source partition map and equality
 * include the nullable TVF name to keep Kafka Connect offsets and source partitions
 * collision-free.
 */
public class SpannerPartition implements Partition {

    private static final String PARTITION_TOKEN_KEY = "partitionToken";
    private static final String TVF_NAME_KEY = "tvfName";

    private final String partitionToken;
    private final String tvfName;

    public SpannerPartition(String partitionToken) {
        this(partitionToken, null);
    }

    public SpannerPartition(String partitionToken, String tvfName) {
        this.partitionToken = partitionToken;
        this.tvfName = tvfName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        Map<String, String> sourcePartition = new LinkedHashMap<>();
        sourcePartition.put(PARTITION_TOKEN_KEY, partitionToken);
        if (tvfName != null && !tvfName.isBlank()) {
            sourcePartition.put(TVF_NAME_KEY, tvfName);
        }
        return Collections.unmodifiableMap(sourcePartition);
    }

    public String toString() {
        if (tvfName == null || tvfName.isBlank()) {
            return "SpannerPartition[{partitionToken=" + partitionToken + "}]";
        }
        return "SpannerPartition[{partitionToken=" + partitionToken + ", tvfName=" + tvfName + "}]";
    }

    public String getValue() {
        return partitionToken;
    }

    public String getTvfName() {
        return tvfName;
    }

    public PartitionKey getKey() {
        return new PartitionKey(partitionToken, tvfName);
    }

    public static String extractToken(Map<String, ?> sourcePartition) {
        return (String) sourcePartition.get(PARTITION_TOKEN_KEY);
    }

    public static String extractTvfName(Map<String, ?> sourcePartition) {
        return (String) sourcePartition.get(TVF_NAME_KEY);
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
        return Objects.equals(partitionToken, that.partitionToken)
                && Objects.equals(tvfName, that.tvfName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionToken, tvfName);
    }
}
