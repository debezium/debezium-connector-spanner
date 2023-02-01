/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.schema;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.model.schema.TableSchema;
import io.debezium.connector.spanner.schema.KafkaSpannerTableSchema;
import io.debezium.connector.spanner.schema.KafkaSpannerTableSchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

class KafkaSpannerSchemaTest {

    @Test
    void getSchema() {
        TableId testTableId = TableId.getTableId("test");

        TopicNamingStrategy<TableId> topicNamingStrategy = Mockito.mock(TopicNamingStrategy.class);
        Mockito.when(topicNamingStrategy.dataChangeTopic(testTableId)).thenReturn("test.Value");

        SchemaNameAdjuster schemaNameAdjuster = Mockito.mock(SchemaNameAdjuster.class);
        Mockito.when(schemaNameAdjuster.adjust("test.Value")).thenReturn("adjust_test.Value");
        Mockito.when(schemaNameAdjuster.adjust("test.Value.Envelope")).thenReturn("adjust_test.Value.Envelope");

        SchemaRegistry schemaRegistry = Mockito.mock(SchemaRegistry.class);
        Mockito.when(schemaRegistry.getWatchedTable(testTableId))
                .thenReturn(new TableSchema(testTableId.getTableName(), List.of()));

        Schema sourceInfoSchema = Schema.OPTIONAL_STRING_SCHEMA;

        KafkaSpannerTableSchemaFactory kafkaSpannerTableSchemaFactory = new KafkaSpannerTableSchemaFactory(
                topicNamingStrategy, schemaNameAdjuster, schemaRegistry, sourceInfoSchema);

        KafkaSpannerTableSchema tableSchema = kafkaSpannerTableSchemaFactory.getTableSchema(testTableId);

        Assertions.assertEquals("adjust_test.Value.Envelope", tableSchema.getEnvelopeSchema().schema().name());
        Assertions.assertEquals(Schema.Type.STRUCT, tableSchema.getEnvelopeSchema().schema().type());
        Assertions.assertLinesMatch(
                List.of("before", "after", "source", "op", "ts_ms", "transaction"),
                tableSchema.getEnvelopeSchema().schema().fields().stream().map(Field::name).collect(Collectors.toList()));
        Assertions.assertEquals(testTableId, tableSchema.id());
        Assertions.assertNull(tableSchema.keySchema());
        Assertions.assertEquals("adjust_test.Value", tableSchema.valueSchema().name());
        Assertions.assertEquals(Schema.Type.STRUCT, tableSchema.valueSchema().type());
    }

}
