/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.leader.rebalancer;

public enum LeaderRebalanceStrategy {
    GREEDY_LEADER, // Leader takes all partitions from obsolete tasks (including shared partitions)
    EQUAL_SHARING // Equal distribution partitions across available tasks
}
