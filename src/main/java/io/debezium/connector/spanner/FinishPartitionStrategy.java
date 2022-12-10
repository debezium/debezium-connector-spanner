/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

/**
 * When to finish Spanner partition:
 * right after streaming was finished for this partition
 * or only after record has been successfully committed to Kafka
 */
public enum FinishPartitionStrategy {
    AFTER_COMMIT, // Provides strong delivery guarantee but adds latency
    AFTER_STREAMING_FINISH // Weaker delivery guarantee but faster
}
