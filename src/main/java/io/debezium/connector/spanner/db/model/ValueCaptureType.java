/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

/**
 * Represents the capture type of a change stream.
 * See https://cloud.google.com/spanner/docs/change-streams#value-capture-type.
 */
public enum ValueCaptureType {
    OLD_AND_NEW_VALUES,
    NEW_ROW,
    NEW_VALUES
}
