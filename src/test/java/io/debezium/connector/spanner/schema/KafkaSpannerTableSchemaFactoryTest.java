/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.schema;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.db.dao.SchemaDao;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.model.schema.TableSchema;
import io.debezium.schema.SchemaNameAdjuster;

class KafkaSpannerTableSchemaFactoryTest {

    @Test
    void testGetTableSchema() {
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        TableSchema tableSchema = mock(TableSchema.class);
        SchemaRegistry schemaRegistry = spy(new SchemaRegistry("Stream Name", new SchemaDao(null), mock(Runnable.class)));
        doReturn(tableSchema).when(schemaRegistry).getWatchedTable(any());

        KafkaSpannerTableSchemaFactory kafkaSpannerTableSchemaFactory = spy(new KafkaSpannerTableSchemaFactory(null,
                schemaNameAdjuster, schemaRegistry, new ConnectSchema(Schema.Type.INT8)));
        doReturn(null).when(kafkaSpannerTableSchemaFactory).buildEnvelope(any(), any());

        assertNotNull(kafkaSpannerTableSchemaFactory.getTableSchema(TableId.getTableId("Table Name")));
    }
}
