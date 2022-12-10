/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import com.google.cloud.spanner.DatabaseClient;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.config.SpannerTableFilter;
import io.debezium.connector.spanner.context.offset.LowWatermarkProvider;
import io.debezium.connector.spanner.context.source.SourceInfo;
import io.debezium.connector.spanner.context.source.SourceInfoFactory;
import io.debezium.connector.spanner.db.dao.SchemaDao;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.exception.SpannerConnectorException;
import io.debezium.connector.spanner.kafka.KafkaPartitionInfoProvider;
import io.debezium.connector.spanner.processor.metadata.SpannerEventMetadataProvider;
import io.debezium.connector.spanner.schema.KafkaSpannerSchema;
import io.debezium.connector.spanner.schema.KafkaSpannerTableSchemaFactory;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.SchemaNameAdjuster;

class SpannerEventDispatcherTest {

    @Test
    void testConstructor() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());

        SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(configuration);
        TopicNamingStrategy<TableId> topicNamingStrategy = (TopicNamingStrategy<TableId>) mock(TopicNamingStrategy.class);
        when(topicNamingStrategy.transactionTopic()).thenReturn("Transaction Topic");

        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        SchemaRegistry schemaRegistry = new SchemaRegistry("Stream Name",
                new SchemaDao(mock(DatabaseClient.class)), mock(Runnable.class));

        KafkaSpannerSchema kafkaSpannerSchema = new KafkaSpannerSchema(new KafkaSpannerTableSchemaFactory(
                topicNamingStrategy, schemaNameAdjuster, schemaRegistry, new ConnectSchema(Schema.Type.INT8)));
        ChangeEventQueue<DataChangeEvent> queue = (ChangeEventQueue<DataChangeEvent>) mock(ChangeEventQueue.class);

        SpannerTableFilter filter = new SpannerTableFilter(new SpannerConnectorConfig(configuration));
        ChangeEventCreator changeEventCreator = mock(ChangeEventCreator.class);
        SpannerEventMetadataProvider metadataProvider = new SpannerEventMetadataProvider();

        SpannerConnectorConfig connectorConfig1 = new SpannerConnectorConfig(configuration);
        TopicNamingStrategy<TableId> topicNamingStrategy2 = (TopicNamingStrategy<TableId>) mock(TopicNamingStrategy.class);
        when(topicNamingStrategy2.heartbeatTopic()).thenReturn("Heartbeat Topic");
        SchemaNameAdjuster schemaNameAdjuster1 = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster1.adjust(any())).thenReturn("Adjust");

        HeartbeatFactory<TableId> heartbeatFactory = new HeartbeatFactory<>(connectorConfig1, topicNamingStrategy2, schemaNameAdjuster1);

        SchemaNameAdjuster schemaNameAdjuster2 = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster2.adjust(any())).thenReturn("Adjust");
        SchemaRegistry schemaRegistry1 = new SchemaRegistry("Stream Name", new SchemaDao(mock(DatabaseClient.class)), mock(Runnable.class));

        SourceInfoFactory sourceInfoFactory = new SourceInfoFactory(new SpannerConnectorConfig(configuration),
                mock(LowWatermarkProvider.class));

        SpannerEventDispatcher actualSpannerEventDispatcher = new SpannerEventDispatcher(connectorConfig,
                topicNamingStrategy, kafkaSpannerSchema, queue, filter, changeEventCreator, metadataProvider,
                heartbeatFactory, schemaNameAdjuster2, schemaRegistry1, sourceInfoFactory, new KafkaPartitionInfoProvider(null));

        assertNull(actualSpannerEventDispatcher.getHistorizedSchema());
        assertSame(kafkaSpannerSchema, actualSpannerEventDispatcher.getSchema());
        verify(topicNamingStrategy).transactionTopic();
        verify(topicNamingStrategy2).heartbeatTopic();
        verify(schemaNameAdjuster1, atLeast(1)).adjust(any());
        verify(schemaNameAdjuster2, atLeast(1)).adjust(any());
    }

    @Test
    void testPublishLowWatermarkStampEventNoTables() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(configuration);
        TopicNamingStrategy<TableId> topicNamingStrategy = (TopicNamingStrategy<TableId>) mock(TopicNamingStrategy.class);
        when(topicNamingStrategy.transactionTopic()).thenReturn("Transaction Topic");
        Configuration configuration1 = mock(Configuration.class);
        when(configuration1.getString((Field) any())).thenReturn("String");
        when(configuration1.getString(anyString())).thenReturn("String");
        when(configuration1.asProperties()).thenReturn(new Properties());
        SpannerTableFilter filter = new SpannerTableFilter(new SpannerConnectorConfig(configuration1));
        Configuration configuration2 = mock(Configuration.class);
        when(configuration2.getString((Field) any())).thenReturn("String");
        when(configuration2.getString(anyString())).thenReturn("String");
        when(configuration2.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig1 = new SpannerConnectorConfig(configuration2);
        TopicNamingStrategy<TableId> topicNamingStrategy1 = (TopicNamingStrategy<TableId>) mock(TopicNamingStrategy.class);
        when(topicNamingStrategy1.heartbeatTopic()).thenReturn("Heartbeat Topic");
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust(any())).thenReturn("Adjust");
        HeartbeatFactory<TableId> heartbeatFactory = new HeartbeatFactory<>(connectorConfig1, topicNamingStrategy1, schemaNameAdjuster);

        SchemaNameAdjuster schemaNameAdjuster1 = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster1.adjust(any())).thenReturn("Adjust");
        Configuration configuration3 = mock(Configuration.class);
        when(configuration3.getString((Field) any())).thenReturn("String");
        when(configuration3.getString(anyString())).thenReturn("String");
        when(configuration3.asProperties()).thenReturn(new Properties());
        SourceInfoFactory sourceInfoFactory = new SourceInfoFactory(new SpannerConnectorConfig(configuration3), mock(LowWatermarkProvider.class));

        TopicNamingStrategy<TableId> topicNamingStrategy2 = (TopicNamingStrategy<TableId>) mock(TopicNamingStrategy.class);
        SchemaNameAdjuster schemaNameAdjuster2 = mock(SchemaNameAdjuster.class);
        SchemaRegistry schemaRegistry = new SchemaRegistry("Stream Name",
                new SchemaDao(mock(DatabaseClient.class)), mock(Runnable.class));

        KafkaSpannerSchema schema = new KafkaSpannerSchema(new KafkaSpannerTableSchemaFactory(topicNamingStrategy2,
                schemaNameAdjuster2, schemaRegistry, new ConnectSchema(Schema.Type.INT8)));
        ChangeEventQueue<DataChangeEvent> queue = (ChangeEventQueue<DataChangeEvent>) mock(ChangeEventQueue.class);
        ChangeEventCreator changeEventCreator = mock(ChangeEventCreator.class);
        SpannerEventMetadataProvider metadataProvider = new SpannerEventMetadataProvider();
        SchemaRegistry schemaRegistry1 = new SchemaRegistry("Stream Name", new SchemaDao(mock(DatabaseClient.class)), mock(Runnable.class));

        assertThrows(SpannerConnectorException.class,
                () -> (new SpannerEventDispatcher(connectorConfig, topicNamingStrategy, schema, queue, filter,
                        changeEventCreator, metadataProvider, heartbeatFactory, schemaNameAdjuster1, schemaRegistry1,
                        sourceInfoFactory, new KafkaPartitionInfoProvider(null))).publishLowWatermarkStampEvent());

        verify(topicNamingStrategy).transactionTopic();
        verify(topicNamingStrategy1).heartbeatTopic();
        verify(schemaNameAdjuster, atLeast(1)).adjust(any());
        verify(schemaNameAdjuster1, atLeast(1)).adjust(any());
    }

    @Test
    void testPublishLowWatermarkStampEvent() throws InterruptedException, ExecutionException {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());

        SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(configuration);
        TopicNamingStrategy<TableId> topicNamingStrategy = (TopicNamingStrategy<TableId>) mock(TopicNamingStrategy.class);
        when(topicNamingStrategy.transactionTopic()).thenReturn("Transaction Topic");
        when(topicNamingStrategy.dataChangeTopic(any())).thenReturn("Change Topic");

        SpannerTableFilter filter = new SpannerTableFilter(new SpannerConnectorConfig(configuration));

        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust(any())).thenReturn("Adjust");
        HeartbeatFactory<TableId> heartbeatFactory = new HeartbeatFactory<>(
                connectorConfig, topicNamingStrategy, schemaNameAdjuster);

        ChangeEventQueue<DataChangeEvent> queue = (ChangeEventQueue<DataChangeEvent>) mock(ChangeEventQueue.class);
        ChangeEventCreator changeEventCreator = mock(ChangeEventCreator.class);
        SpannerEventMetadataProvider metadataProvider = new SpannerEventMetadataProvider();

        SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
        TableId tableId = mock(TableId.class);
        when(schemaRegistry.getAllTables()).thenReturn(Set.of(tableId));

        KafkaSpannerSchema schema = mock(KafkaSpannerSchema.class);
        DataCollectionSchema collectionSchema = mock(DataCollectionSchema.class);
        when(schema.schemaFor(any())).thenReturn(collectionSchema);
        SourceInfoFactory sourceInfoFactory = mock(SourceInfoFactory.class);
        SourceInfo sourceInfo = mock(SourceInfo.class);
        when(sourceInfoFactory.getSourceInfoForLowWatermarkStamp(any())).thenReturn(sourceInfo);
        Struct struct = mock(Struct.class);
        when(sourceInfo.struct()).thenReturn(struct);

        KafkaPartitionInfoProvider kafkaPartitionInfoProvider = mock(KafkaPartitionInfoProvider.class);
        when(kafkaPartitionInfoProvider.getPartitions(anyString())).thenReturn(List.of(1));

        SpannerEventDispatcher spannerEventDispatcher = spy(new SpannerEventDispatcher(
                connectorConfig, topicNamingStrategy, schema, queue, filter, changeEventCreator, metadataProvider,
                heartbeatFactory, schemaNameAdjuster, schemaRegistry, sourceInfoFactory, kafkaPartitionInfoProvider));

        SourceRecord sourceRecord = mock(SourceRecord.class);
        doReturn(sourceRecord).when(spannerEventDispatcher).emitSourceRecord(anyString(), any(), anyInt(), any());

        spannerEventDispatcher.publishLowWatermarkStampEvent();
        spannerEventDispatcher.destroy();
        spannerEventDispatcher.close();

        verify(topicNamingStrategy).dataChangeTopic(tableId);
        verify(queue).enqueue(any());
    }
}
