/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

/**
 * What to do if partition get stuck
 */
public enum StuckPartitionStrategy {
    REPEAT_STREAMING, // retry continuously
    ESCALATE // fail the task
}
