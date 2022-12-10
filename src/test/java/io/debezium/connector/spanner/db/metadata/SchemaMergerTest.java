/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.metadata;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.db.model.schema.Column;
import io.debezium.connector.spanner.db.model.schema.ColumnType;
import io.debezium.connector.spanner.db.model.schema.DataType;
import io.debezium.connector.spanner.db.model.schema.SpannerSchema;
import io.debezium.connector.spanner.db.model.schema.TableSchema;

class SchemaMergerTest {

    @Test
    void mergeAddNewColumn() {
        SpannerSchema schema = buildTestSchema(Map.of("test_table",
                List.of(
                        new Column("id", new ColumnType(DataType.INT64), true, 0, false),
                        new Column("name", new ColumnType(DataType.STRING), false, 1, true))));

        SpannerSchema newSchema = buildTestSchema(Map.of("test_table",
                List.of(
                        new Column("id", new ColumnType(DataType.INT64), true, 0, false),
                        new Column("name", new ColumnType(DataType.STRING), false, 1, true),
                        new Column("belka", new ColumnType(DataType.STRING), false, 2, true))));

        SpannerSchema mergedSchema = SchemaMerger.merge(schema, newSchema);

        Assertions.assertNotNull(mergedSchema.getTable(TableId.getTableId("test_table")).getColumn("belka"));
    }

    @Test
    void mergeDeleteColumn() {
        SpannerSchema schema = buildTestSchema(Map.of("test_table",
                List.of(
                        new Column("id", new ColumnType(DataType.INT64), true, 0, false),
                        new Column("name", new ColumnType(DataType.STRING), false, 1, true),
                        new Column("belka", new ColumnType(DataType.STRING), false, 2, true))));

        SpannerSchema newSchema = buildTestSchema(Map.of("test_table",
                List.of(
                        new Column("id", new ColumnType(DataType.INT64), true, 0, false),
                        new Column("name", new ColumnType(DataType.STRING), false, 1, true))));

        SpannerSchema mergedSchema = SchemaMerger.merge(schema, newSchema);

        Assertions.assertNotNull(mergedSchema.getTable(TableId.getTableId("test_table")).getColumn("belka"));
    }

    @Test
    void mergeChangeTypeOfColumn() {
        SpannerSchema schema = buildTestSchema(Map.of("test_table",
                List.of(
                        new Column("id", new ColumnType(DataType.INT64), true, 0, false),
                        new Column("name", new ColumnType(DataType.STRING), false, 1, true),
                        new Column("belka", new ColumnType(DataType.BYTES), false, 2, true))));

        SpannerSchema newSchema = buildTestSchema(Map.of("test_table",
                List.of(
                        new Column("id", new ColumnType(DataType.INT64), true, 0, false),
                        new Column("name", new ColumnType(DataType.STRING), false, 1, true),
                        new Column("belka", new ColumnType(DataType.STRING), false, 2, true))));

        SpannerSchema mergedSchema = SchemaMerger.merge(schema, newSchema);

        Assertions.assertEquals(DataType.STRING,
                mergedSchema.getTable(TableId.getTableId("test_table")).getColumn("belka").getType().getType());
    }

    @Test
    void mergeNewTable() {
        SpannerSchema schema = buildTestSchema(Map.of("test_table1",
                List.of(
                        new Column("id", new ColumnType(DataType.INT64), true, 0, false),
                        new Column("name", new ColumnType(DataType.STRING), false, 1, true))));

        SpannerSchema newSchema = buildTestSchema(Map.of("test_table2",
                List.of(
                        new Column("id", new ColumnType(DataType.INT64), true, 0, false),
                        new Column("test", new ColumnType(DataType.STRING), false, 1, true))));

        SpannerSchema mergedSchema = SchemaMerger.merge(schema, newSchema);

        Assertions.assertEquals(1, mergedSchema.getAllTables().size());
        Assertions.assertNotNull(mergedSchema.getTable(TableId.getTableId("test_table2")));
    }

    SpannerSchema buildTestSchema(Map<String, List<Column>> definition) {
        Map<TableId, TableSchema> tables = definition.entrySet().stream().map(entry -> {
            TableSchema tableSchema = new TableSchema(entry.getKey(), entry.getValue());
            return new AbstractMap.SimpleEntry<>(TableId.getTableId(entry.getKey()), tableSchema);
        }).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        return new SpannerSchema(tables);
    }
}
