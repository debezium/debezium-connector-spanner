/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.metadata;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.spanner.Dialect;
import io.debezium.connector.spanner.db.model.schema.Column;
import io.debezium.connector.spanner.db.model.schema.TableSchema;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SchemaValidatorTest {

    @Test
    void testValidate() {
        TableSchema schemaTable = new TableSchema("Name", new ArrayList<>());
        TableSchema watchedTable = new TableSchema("Name", new ArrayList<>());
        assertTrue(SchemaValidator.validate(schemaTable, watchedTable, new ArrayList<>()));
    }

    @Test
    void testNotValidate() {
        TableSchema schemaTable = new TableSchema("Name1", new ArrayList<>());
        TableSchema watchedTable = new TableSchema("Name2", new ArrayList<>());
        List<Column> rowTypes = new ArrayList<>();
        rowTypes.add(Column.create("name1", "BOOL", true, 1L, false, Dialect.GOOGLE_STANDARD_SQL));
        assertFalse(SchemaValidator.validate(schemaTable, watchedTable, rowTypes));
    }
}
