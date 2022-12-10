/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

class TableIdTest {

    @Test
    void testGetTableId() {
        TableId actualTableId = TableId.getTableId("Table Name");
        String actualToStringResult = actualTableId.toString();
        assertEquals("Table Name", actualTableId.getTableName());
        assertEquals("Table Name", actualToStringResult);
    }

    @Test
    void testCreation() {
        TableId tableId = TableId.getTableId("Table Name");
        assertEquals("Table Name", tableId.getTableName());
        assertEquals("Table Name", tableId.identifier());
    }

    @Test
    void testParts() {
        List<String> actualPartsResult = TableId.getTableId("Table Name").parts();
        assertEquals(1, actualPartsResult.size());
        assertEquals("Table Name", actualPartsResult.get(0));
    }

    @Test
    void testDatabaseParts() {
        List<String> actualDatabasePartsResult = TableId.getTableId("Table Name").databaseParts();
        assertEquals(1, actualDatabasePartsResult.size());
        assertEquals("Table Name", actualDatabasePartsResult.get(0));
    }

    @Test
    void testSchemaParts() {
        List<String> actualSchemaPartsResult = TableId.getTableId("Table Name").schemaParts();
        assertEquals(1, actualSchemaPartsResult.size());
        assertEquals("Table Name", actualSchemaPartsResult.get(0));
    }
}
