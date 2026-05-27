/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.google.cloud.spanner.Dialect;

class ColumnTest {

    @Test
    void testIsNullable() {
        assertTrue(Column.create("Name", "BOOL", true, 1L, true, Dialect.GOOGLE_STANDARD_SQL)
                .isNullable());
        assertFalse(Column.create("Name", "BOOL", true, 1L, false, Dialect.GOOGLE_STANDARD_SQL)
                .isNullable());
    }

    @Test
    void testCreateException() {
        assertThrows(IllegalArgumentException.class,
                () -> Column.create("Name", "Spanner Type", true, 1L, true,
                        Dialect.GOOGLE_STANDARD_SQL));
    }

    @Test
    void testCreate() {
        Column actualCreateResult = Column.create("Name", "INT64", true, 1L, true,
                Dialect.GOOGLE_STANDARD_SQL);
        assertEquals("Name", actualCreateResult.getName());
        assertTrue(actualCreateResult.isPrimaryKey());
        assertEquals(1L, actualCreateResult.getOrdinalPosition());
        assertEquals(DataType.INT64, actualCreateResult.getType().getType());
    }

    @Test
    void testCreateStringGoogleStandardSql() {
        Column actualCreateResult = Column.create("Name", "STRING", true, 1L, true,
                Dialect.GOOGLE_STANDARD_SQL);
        assertEquals(DataType.STRING, actualCreateResult.getType().getType());
    }

    @Test
    void testCreateStringPostgresql() {
        Column actualCreateResult = Column.create("Name", "CHARACTER VARYING", true, 1L, true,
                Dialect.POSTGRESQL);
        assertEquals(DataType.STRING, actualCreateResult.getType().getType());
    }

    @Test
    void testCreateUuidGoogleStandardSql() {
        Column actualCreateResult = Column.create("Name", "UUID", true, 1L, true,
                Dialect.GOOGLE_STANDARD_SQL);
        assertEquals(DataType.UUID, actualCreateResult.getType().getType());
    }

    @Test
    void testCreateUuidPostgresql() {
        Column actualCreateResult = Column.create("Name", "UUID", true, 1L, true,
                Dialect.POSTGRESQL);
        assertEquals(DataType.UUID, actualCreateResult.getType().getType());
    }
}
