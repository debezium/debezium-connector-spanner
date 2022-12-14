/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

/**
 * Represents the capture type of a change stream.
 * OLD_AND_NEW_VALUES: captures the column values before and after the
 *  database operations were applied.
 * NEW_ROW: captures the new values for the entire tracked row:
 *  For inserts, returns all the inserted columns.
 *  For updates, returns new values for both changed and unchanged columns for the entire tracked row. Does not return the old values.
 *  For deletes, returns nothing
 * NEW_VALUES: only captures new values for the changed columns
 *  For inserts, returns all the inserted columns.
 *  For updates, returns new values for the changed columns. Does not return the old values.
 *  For deletes, returns nothing
 */
public enum ValueCaptureType {
    OLD_AND_NEW_VALUES,
    NEW_ROW,
    NEW_VALUES
}
