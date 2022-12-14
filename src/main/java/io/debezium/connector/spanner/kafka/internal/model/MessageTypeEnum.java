/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

public enum MessageTypeEnum {
    REGULAR, // this message is published, after the task state was changed
    NEW_EPOCH, // leader initiates new epoch, when answers from all members have been received
    UPDATE_EPOCH, // periodical message to update epoch offset
    REBALANCE_ANSWER // all tasks publish their states on Rebalance Event to show they are alive
}