/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.schema;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import com.google.cloud.spanner.DatabaseClient;

import io.debezium.connector.spanner.db.dao.SchemaDao;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.SchemaNameAdjuster;

class KafkaSpannerSchemaTest {

    @Test
    void testIsHistorized() {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        SchemaRegistry schemaRegistry = new SchemaRegistry("Stream Name", new SchemaDao(null), mock(Runnable.class));

        KafkaSpannerSchema actualKafkaSpannerSchema = new KafkaSpannerSchema(new KafkaSpannerTableSchemaFactory(null,
                schemaNameAdjuster, schemaRegistry, new ConnectSchema(Schema.Type.INT8)));
        actualKafkaSpannerSchema.close();
        assertFalse(actualKafkaSpannerSchema.isHistorized());
    }

    @Test
    void testSchemaFor() {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        SchemaRegistry schemaRegistry = new SchemaRegistry("Stream Name", new SchemaDao(null), mock(Runnable.class));

        KafkaSpannerTableSchemaFactory kafkaSpannerTableSchemaFactory = spy(new KafkaSpannerTableSchemaFactory(null,
                schemaNameAdjuster, schemaRegistry, new ConnectSchema(Schema.Type.INT8)));

        KafkaSpannerTableSchema kafkaSpannerTableSchema = mock(KafkaSpannerTableSchema.class);
        doReturn(kafkaSpannerTableSchema).when(kafkaSpannerTableSchemaFactory).getTableSchema(any());
        KafkaSpannerSchema kafkaSpannerSchema = new KafkaSpannerSchema(kafkaSpannerTableSchemaFactory);

        assertNotNull(kafkaSpannerSchema.schemaFor(TableId.getTableId("Table Name")));
    }

    @Test
    void testSchemaForThrow() {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        SchemaRegistry schemaRegistry = new SchemaRegistry("Stream Name", new SchemaDao(null), mock(Runnable.class));

        KafkaSpannerSchema kafkaSpannerSchema = new KafkaSpannerSchema(new KafkaSpannerTableSchemaFactory(null,
                schemaNameAdjuster, schemaRegistry, new ConnectSchema(Schema.Type.INT8)));
        assertThrows(IllegalStateException.class, () -> kafkaSpannerSchema
                .schemaFor(TableId.getTableId("Table Name")));
    }

    @Test
    void testTableInformationComplete() {
        TopicNamingStrategy<TableId> topicNamingStrategy = (TopicNamingStrategy<TableId>) mock(TopicNamingStrategy.class);
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        SchemaRegistry schemaRegistry = new SchemaRegistry("Stream Name", new SchemaDao(mock(DatabaseClient.class)), mock(Runnable.class));

        assertFalse((new KafkaSpannerSchema(new KafkaSpannerTableSchemaFactory(topicNamingStrategy, schemaNameAdjuster,
                schemaRegistry, new ConnectSchema(Schema.Type.INT8)))).tableInformationComplete());
    }
}
