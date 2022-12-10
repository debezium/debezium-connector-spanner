/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.schema;

/**
 * Data types supported by Spanner
 */
public enum DataType {
    BOOL,
    INT64,
    FLOAT64,
    TIMESTAMP,
    DATE,
    STRING,
    BYTES,
    ARRAY,
    STRUCT,
    NUMERIC,
    JSON,
}
