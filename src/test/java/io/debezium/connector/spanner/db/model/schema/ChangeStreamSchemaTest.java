/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ChangeStreamSchemaTest {

    private static Stream<Arguments> tableListProvider() {
        return Stream.of(
                Arguments.of(List.of()),
                Arguments.of(List.of(new ChangeStreamSchema.Table("Name", true))));
    }

    @ParameterizedTest
    @MethodSource("tableListProvider")
    void testChangeStream(List<ChangeStreamSchema.Table> tableList) {
        ChangeStreamSchema actualChangeStream = new ChangeStreamSchema("Name", true, tableList);
        assertEquals("Name", actualChangeStream.getName());
        assertTrue(actualChangeStream.isWatchedAllTables());
    }

    @Test
    void testBuilder() {
        ChangeStreamSchema actualBuildResult = ChangeStreamSchema.builder().allTables(true).name("Name").build();
        assertEquals("Name", actualBuildResult.getName());
        assertTrue(actualBuildResult.isWatchedAllTables());
        assertTrue(actualBuildResult.getTables().isEmpty());
    }

    @Test
    void testGetTables() {
        assertTrue(new ChangeStreamSchema("Name", false, new ArrayList<>()).getTables().isEmpty());
    }

    @Test
    void testGetAllTables() {
        assertTrue(new ChangeStreamSchema("Name", true, new ArrayList<>()).getTables().isEmpty());
    }

    @Test
    void testGetTable() {
        assertNull(new ChangeStreamSchema("Name", false, new ArrayList<>()).getTable("Name"));
        assertNotNull(new ChangeStreamSchema("Name", true, new ArrayList<>()).getTable("Name"));
    }

    @Test
    void testGetName() {
        ChangeStreamSchema changeStream = new ChangeStreamSchema("Name", true, new ArrayList<>());
        assertEquals("Name", changeStream.getTable("Name").getName());
    }

    @Test
    void testTableConstructor() {
        assertEquals("Name", new ChangeStreamSchema.Table("Name", true).getName());
    }

    @Test
    void testTableHasAllColumns() {
        assertTrue(new ChangeStreamSchema.Table("Name", true).hasColumn("Column"));
    }

    @Test
    void testTableHasColumn() {
        assertFalse(new ChangeStreamSchema.Table("Name", false).hasColumn("Column"));
    }
}
