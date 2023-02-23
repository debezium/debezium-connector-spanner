/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.schema;

import java.util.function.Function;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.model.Mod;
import io.debezium.connector.spanner.db.model.schema.Column;
import io.debezium.connector.spanner.db.model.schema.TableSchema;
import io.debezium.connector.spanner.schema.mapper.ColumnTypeSchemaMapper;
import io.debezium.connector.spanner.schema.mapper.FieldJsonNodeValueMapper;
import io.debezium.data.Envelope;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Builds Kafka record schema for Spanner DB table
 */
public class KafkaSpannerTableSchemaFactory {
    private final TopicNamingStrategy<TableId> topicNamingStrategy;

    private final SchemaNameAdjuster schemaNameAdjuster;

    private final SchemaRegistry schemaRegistry;

    private final Schema sourceInfoSchema;

    public KafkaSpannerTableSchemaFactory(TopicNamingStrategy<TableId> topicNamingStrategy, SchemaNameAdjuster schemaNameAdjuster,
                                          SchemaRegistry schemaRegistry, Schema sourceInfoSchema) {
        this.topicNamingStrategy = topicNamingStrategy;
        this.schemaNameAdjuster = schemaNameAdjuster;
        this.schemaRegistry = schemaRegistry;
        this.sourceInfoSchema = sourceInfoSchema;
    }

    public KafkaSpannerTableSchema getTableSchema(TableId tableId) {

        TableSchema table = schemaRegistry.getWatchedTable(tableId);

        Schema valueSchema = buildValueSchema(table);
        Schema keySchema = buildKeySchema(table);

        Envelope envelope = buildEnvelope(tableId, valueSchema);

        Function<Mod, Struct> keyGenerator = getKeyStructGenerator(keySchema, table);
        Function<Mod, Struct> valueOldGenerator = getOldValueStructGenerator(valueSchema, table);

        Function<Mod, Struct> valueNewGenerator = getNewValueStructGenerator(valueSchema, table);

        return new KafkaSpannerTableSchema(tableId, keySchema, keyGenerator, envelope, valueSchema, valueOldGenerator,
                valueNewGenerator);
    }

    private Schema buildKeySchema(TableSchema table) {

        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct()
                .name(schemaNameAdjuster.adjust(table.getName() + ".Key"));

        boolean hasPrimaryKey = false;

        for (Column column : table.keyColumns()) {
            keySchemaBuilder.field(column.getName(),
                    ColumnTypeSchemaMapper.getSchema(column.getType(), column.isNullable()));
            hasPrimaryKey = true;
        }

        return hasPrimaryKey ? keySchemaBuilder.build() : null;
    }

    private Schema buildValueSchema(TableSchema table) {
        SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct()
                .name(schemaNameAdjuster.adjust(table.getName() + ".Value"));
        for (Column column : table.columns()) {
            valueSchemaBuilder.field(column.getName(),
                    ColumnTypeSchemaMapper.getSchema(column.getType(), column.isNullable() || !column.isPrimaryKey()));
        }
        return valueSchemaBuilder.optional().build();
    }

    @VisibleForTesting
    Envelope buildEnvelope(TableId tableId, Schema valueSchema) {
        String envelopeSchemaName = Envelope.schemaName(topicNamingStrategy.dataChangeTopic(tableId));

        return Envelope.defineSchema()
                .withName(schemaNameAdjuster.adjust(envelopeSchemaName))
                .withRecord(valueSchema)
                .withSource(sourceInfoSchema)
                .build();
    }

    private Function<Mod, Struct> getKeyStructGenerator(Schema keySchema, TableSchema table) {
        return mod -> {
            Struct keyStruct = new Struct(keySchema);
            table.keyColumns().forEach(column -> {
                Field field = keySchema.field(column.getName());
                JsonNode node = mod.getKeyJsonNode(column.getName());
                keyStruct.put(field, FieldJsonNodeValueMapper.getValue(field, node));
            });
            return keyStruct;
        };
    }

    private Function<Mod, Struct> getOldValueStructGenerator(Schema valueSchema, TableSchema table) {
        return mod -> {
            Struct valueStruct = new Struct(valueSchema);
            table.columns().forEach(column -> {
                Field field = valueSchema.field(column.getName());
                JsonNode node = mod.getOldValueNode(column.getName());
                if (node != null) {
                    valueStruct.put(field, FieldJsonNodeValueMapper.getValue(field, node));
                }
            });
            return valueStruct;
        };
    }

    private Function<Mod, Struct> getNewValueStructGenerator(Schema valueSchema, TableSchema table) {
        return mod -> {
            Struct valueStruct = new Struct(valueSchema);
            table.columns().forEach(column -> {
                Field field = valueSchema.field(column.getName());
                JsonNode node = mod.getNewValueNode(column.getName());
                if (node != null) {
                    valueStruct.put(field, FieldJsonNodeValueMapper.getValue(field, node));
                }
            });
            return valueStruct;
        };
    }
}
