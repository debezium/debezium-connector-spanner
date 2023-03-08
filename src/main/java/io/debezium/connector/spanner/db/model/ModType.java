/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import io.debezium.connector.spanner.db.model.event.DataChangeEvent;

/**
 * Represents the type of modification applied in the {@link DataChangeEvent}. It can be one of the
 * following: INSERT, UPDATE or DELETE.
 */
public enum ModType {
    INSERT,
    UPDATE,
    DELETE,
    UNKNOWN
}
