/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

/**
 * The partition mode a change stream is created with. See the Spanner change
 * streams documentation for {@code partition_mode}.
 */
public enum PartitionMode {
    IMMUTABLE_KEY_RANGE,
    MUTABLE_KEY_RANGE
}
