/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.state;

/**
 * Notifies that state of the task has been changed
 * and other tasks should be aware of it
 */
public class SyncEvent implements TaskStateChangeEvent {
}
