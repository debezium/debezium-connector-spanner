/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.metadata;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.db.model.schema.Column;
import io.debezium.connector.spanner.db.model.schema.TableSchema;

/** Validates incoming row against stored schema */
public class SchemaValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaValidator.class);

    private SchemaValidator() {
    }

    public static boolean validate(
                                   TableSchema schemaTable, TableSchema watchedTable, List<Column> rowType) {

        final String tableName = schemaTable.getName();

        for (Column column : rowType) {
            final String columnName = tableName + "." + column.getName();

            final Column schemaColumn = schemaTable.getColumn(column.getName());

            if (schemaColumn == null) {
                LOGGER.warn("Column not found in registry : {}", columnName);
                return false;
            }

            if (watchedTable.getColumn(schemaColumn.getName()) == null) {
                LOGGER.warn("ChangeStream doesn't watch column: {}", columnName);
                return false;
            }

            if (schemaColumn.isPrimaryKey() != column.isPrimaryKey()) {
                throw new IllegalStateException(columnName + " primary key data is incorrect in registry");
            }

            if (!schemaColumn.getType().equals(column.getType())) {
                LOGGER.warn("{} type is incorrect in registry", columnName);
                return false;
            }
        }
        return true;
    }
}
