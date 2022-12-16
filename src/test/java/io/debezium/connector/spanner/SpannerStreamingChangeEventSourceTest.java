/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.spanner.config.SpannerTableFilter;
import io.debezium.connector.spanner.context.offset.LowWatermarkProvider;
import io.debezium.connector.spanner.context.offset.SpannerOffsetContextFactory;
import io.debezium.connector.spanner.context.source.SourceInfoFactory;
import io.debezium.connector.spanner.db.dao.SchemaDao;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.model.ModType;
import io.debezium.connector.spanner.db.model.StreamEventMetadata;
import io.debezium.connector.spanner.db.model.ValueCaptureType;
import io.debezium.connector.spanner.db.model.schema.Column;
import io.debezium.connector.spanner.kafka.KafkaPartitionInfoProvider;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.connector.spanner.processor.metadata.SpannerEventMetadataProvider;
import io.debezium.connector.spanner.schema.KafkaSpannerSchema;
import io.debezium.connector.spanner.schema.KafkaSpannerTableSchemaFactory;
import io.debezium.connector.spanner.task.SynchronizedPartitionManager;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;
import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.SchemaNameAdjuster;

class SpannerStreamingChangeEventSourceTest {

    @Disabled
    @Test
    void testExecute() throws InterruptedException {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(configuration);
        ChangeEventQueue<?> changeEventQueue = (ChangeEventQueue<?>) mock(ChangeEventQueue.class);
        doNothing().when(changeEventQueue).producerException((RuntimeException) any());
        ErrorHandler errorHandler = new ErrorHandler(SourceConnector.class, connectorConfig, changeEventQueue);

        Configuration configuration1 = mock(Configuration.class);
        when(configuration1.getString((Field) any())).thenReturn("String");
        when(configuration1.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig1 = new SpannerConnectorConfig(configuration1);
        TopicNamingStrategy<TableId> topicNamingStrategy = (TopicNamingStrategy<TableId>) mock(TopicNamingStrategy.class);
        when(topicNamingStrategy.transactionTopic()).thenReturn("Transaction Topic");
        Configuration configuration2 = mock(Configuration.class);
        when(configuration2.getString((Field) any())).thenReturn("String");
        when(configuration2.asProperties()).thenReturn(new Properties());
        SpannerTableFilter filter = new SpannerTableFilter(new SpannerConnectorConfig(configuration2));
        Configuration configuration3 = mock(Configuration.class);
        when(configuration3.getString((Field) any())).thenReturn("String");
        when(configuration3.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig2 = new SpannerConnectorConfig(configuration3);
        TopicNamingStrategy<TableId> topicNamingStrategy1 = (TopicNamingStrategy<TableId>) mock(
                TopicNamingStrategy.class);
        when(topicNamingStrategy1.heartbeatTopic()).thenReturn("Heartbeat Topic");
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust((String) any())).thenReturn("Adjust");
        HeartbeatFactory<TableId> heartbeatFactory = new HeartbeatFactory<>(connectorConfig2, topicNamingStrategy1,
                schemaNameAdjuster);

        SchemaNameAdjuster schemaNameAdjuster1 = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster1.adjust((String) any())).thenReturn("Adjust");
        Configuration configuration4 = mock(Configuration.class);
        when(configuration4.getString((Field) any())).thenReturn("String");
        when(configuration4.asProperties()).thenReturn(new Properties());
        SourceInfoFactory sourceInfoFactory = new SourceInfoFactory(new SpannerConnectorConfig(configuration4),
                mock(LowWatermarkProvider.class));

        TopicNamingStrategy<TableId> topicNamingStrategy2 = (TopicNamingStrategy<TableId>) mock(
                TopicNamingStrategy.class);
        SchemaNameAdjuster schemaNameAdjuster2 = mock(SchemaNameAdjuster.class);
        SchemaRegistry schemaRegistry = new SchemaRegistry("Stream Name", mock(SchemaDao.class), mock(Runnable.class));

        KafkaSpannerSchema schema = new KafkaSpannerSchema(new KafkaSpannerTableSchemaFactory(topicNamingStrategy2,
                schemaNameAdjuster2, schemaRegistry, new ConnectSchema(Schema.Type.INT8)));
        ChangeEventQueue<DataChangeEvent> queue = (ChangeEventQueue<DataChangeEvent>) mock(ChangeEventQueue.class);
        ChangeEventCreator changeEventCreator = mock(ChangeEventCreator.class);
        SpannerEventMetadataProvider metadataProvider = new SpannerEventMetadataProvider();
        SchemaRegistry schemaRegistry1 = new SchemaRegistry("Stream Name", new SchemaDao(mock(DatabaseClient.class)), mock(Runnable.class));

        SpannerEventDispatcher spannerEventDispatcher = new SpannerEventDispatcher(connectorConfig1, topicNamingStrategy,
                schema, queue, filter, changeEventCreator, metadataProvider, heartbeatFactory, schemaNameAdjuster1,
                schemaRegistry1, sourceInfoFactory, new KafkaPartitionInfoProvider(null));

        StreamEventQueue eventQueue = new StreamEventQueue(3, new MetricsEventPublisher());

        MetricsEventPublisher metricsEventPublisher = new MetricsEventPublisher();
        SynchronizedPartitionManager partitionManager = new SynchronizedPartitionManager(
                (BlockingConsumer<TaskStateChangeEvent>) mock(BlockingConsumer.class));
        SpannerStreamingChangeEventSource spannerStreamingChangeEventSource = new SpannerStreamingChangeEventSource(
                errorHandler, null, eventQueue, metricsEventPublisher, partitionManager, new SchemaRegistry("Stream Name",
                        new SchemaDao(mock(DatabaseClient.class)), mock(Runnable.class)),
                spannerEventDispatcher, true, mock(SpannerOffsetContextFactory.class));
        Configuration configuration5 = mock(Configuration.class);
        when(configuration5.getString((Field) any())).thenReturn("String");
        when(configuration5.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig3 = new SpannerConnectorConfig(configuration5);
        ErrorHandler errorHandler1 = new ErrorHandler(SourceConnector.class, connectorConfig3,
                (ChangeEventQueue<?>) mock(ChangeEventQueue.class));

        Configuration configuration6 = mock(Configuration.class);
        when(configuration6.getString((Field) any())).thenReturn("String");
        when(configuration6.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig4 = new SpannerConnectorConfig(configuration6);
        Class<SourceConnector> connectorType = SourceConnector.class;
        ChangeEventSourceFactory<Partition, OffsetContext> changeEventSourceFactory = (ChangeEventSourceFactory<Partition, OffsetContext>) mock(
                ChangeEventSourceFactory.class);
        ChangeEventSourceCoordinator<Partition, OffsetContext>.ChangeEventSourceContextImpl context = (new ChangeEventSourceCoordinator<>(
                null, errorHandler1, connectorType, connectorConfig4, changeEventSourceFactory,
                new DefaultChangeEventSourceMetricsFactory<>(), (EventDispatcher<Partition, ?>) mock(EventDispatcher.class),
                (DatabaseSchema<?>) mock(DatabaseSchema.class))).new ChangeEventSourceContextImpl();
        SpannerPartition partition = SpannerPartition.getInitialSpannerPartition();
        Configuration configuration7 = mock(Configuration.class);
        when(configuration7.getString((Field) any())).thenReturn("String");
        when(configuration7.asProperties()).thenReturn(new Properties());

        spannerStreamingChangeEventSource.execute(context, partition, null);

        verify(configuration).getString((Field) any());
        verify(configuration).asProperties();
        verify(changeEventQueue).producerException((RuntimeException) any());
        verify(configuration1).getString((Field) any());
        verify(configuration1).asProperties();
        verify(topicNamingStrategy).transactionTopic();
        verify(configuration2).getString((Field) any());
        verify(configuration2).asProperties();
        verify(configuration3).getString((Field) any());
        verify(configuration3).asProperties();
        verify(topicNamingStrategy1).heartbeatTopic();
        verify(schemaNameAdjuster, atLeast(1)).adjust((String) any());
        verify(schemaNameAdjuster1, atLeast(1)).adjust((String) any());
        verify(configuration4).getString((Field) any());
        verify(configuration4).asProperties();
        verify(configuration5).getString((Field) any());
        verify(configuration5).asProperties();
        verify(configuration6).getString((Field) any());
        verify(configuration6).asProperties();
        verify(configuration7).getString((Field) any());
        verify(configuration7).asProperties();
    }

    @Test
    void testProcessDataChangeEvent() throws InterruptedException {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((String) any())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(configuration);
        ErrorHandler errorHandler = new ErrorHandler(SourceConnector.class, connectorConfig,
                (ChangeEventQueue<?>) mock(ChangeEventQueue.class));

        Configuration configuration1 = mock(Configuration.class);
        when(configuration1.getString((String) any())).thenReturn("String");
        when(configuration1.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig1 = new SpannerConnectorConfig(configuration1);
        TopicNamingStrategy<TableId> topicNamingStrategy = (TopicNamingStrategy<TableId>) mock(TopicNamingStrategy.class);
        when(topicNamingStrategy.transactionTopic()).thenReturn("Transaction Topic");
        Configuration configuration2 = mock(Configuration.class);
        when(configuration2.getString((String) any())).thenReturn("String");
        when(configuration2.asProperties()).thenReturn(new Properties());
        SpannerTableFilter filter = new SpannerTableFilter(new SpannerConnectorConfig(configuration2));
        Configuration configuration3 = mock(Configuration.class);
        when(configuration3.getString((String) any())).thenReturn("String");
        when(configuration3.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig connectorConfig2 = new SpannerConnectorConfig(configuration3);
        TopicNamingStrategy<TableId> topicNamingStrategy1 = (TopicNamingStrategy<TableId>) mock(TopicNamingStrategy.class);
        when(topicNamingStrategy1.heartbeatTopic()).thenReturn("Heartbeat Topic");
        SchemaNameAdjuster schemaNameAdjuster = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster.adjust((String) any())).thenReturn("Adjust");
        HeartbeatFactory<TableId> heartbeatFactory = new HeartbeatFactory<>(
                connectorConfig2, topicNamingStrategy1, schemaNameAdjuster);

        SchemaNameAdjuster schemaNameAdjuster1 = mock(SchemaNameAdjuster.class);
        when(schemaNameAdjuster1.adjust((String) any())).thenReturn("Adjust");
        Configuration configuration4 = mock(Configuration.class);
        when(configuration4.getString((String) any())).thenReturn("String");
        when(configuration4.asProperties()).thenReturn(new Properties());
        SourceInfoFactory sourceInfoFactory = new SourceInfoFactory(new SpannerConnectorConfig(configuration4),
                mock(LowWatermarkProvider.class));

        TopicNamingStrategy<TableId> topicNamingStrategy2 = (TopicNamingStrategy<TableId>) mock(TopicNamingStrategy.class);
        SchemaNameAdjuster schemaNameAdjuster2 = mock(SchemaNameAdjuster.class);
        SchemaRegistry schemaRegistry = new SchemaRegistry("Stream Name", mock(SchemaDao.class), mock(Runnable.class));

        KafkaSpannerSchema schema = new KafkaSpannerSchema(new KafkaSpannerTableSchemaFactory(topicNamingStrategy2,
                schemaNameAdjuster2, schemaRegistry, new ConnectSchema(Schema.Type.INT8)));
        ChangeEventQueue<io.debezium.pipeline.DataChangeEvent> queue = (ChangeEventQueue<io.debezium.pipeline.DataChangeEvent>) mock(ChangeEventQueue.class);
        ChangeEventCreator changeEventCreator = mock(ChangeEventCreator.class);
        SpannerEventMetadataProvider metadataProvider = new SpannerEventMetadataProvider();
        SchemaRegistry schemaRegistryDatabaseClient = spy(new SchemaRegistry(
                "Stream Name", new SchemaDao(mock(DatabaseClient.class)), mock(Runnable.class)));

        SpannerEventDispatcher spannerEventDispatcher = new SpannerEventDispatcher(connectorConfig1, topicNamingStrategy,
                schema, queue, filter, changeEventCreator, metadataProvider, heartbeatFactory, schemaNameAdjuster1,
                schemaRegistryDatabaseClient, sourceInfoFactory, new KafkaPartitionInfoProvider(null));

        Configuration configuration5 = mock(Configuration.class);
        when(configuration5.getString((String) any())).thenReturn("String");
        when(configuration5.asProperties()).thenReturn(new Properties());
        SpannerOffsetContextFactory offsetContextFactory = new SpannerOffsetContextFactory(
                new SourceInfoFactory(new SpannerConnectorConfig(configuration5), mock(LowWatermarkProvider.class)));
        StreamEventQueue eventQueue = new StreamEventQueue(3, new MetricsEventPublisher());

        MetricsEventPublisher metricsEventPublisher = new MetricsEventPublisher();
        SynchronizedPartitionManager partitionManager = new SynchronizedPartitionManager(
                (BlockingConsumer<TaskStateChangeEvent>) mock(BlockingConsumer.class));

        doNothing().when(schemaRegistryDatabaseClient).checkSchema(any(), any(), any());

        SpannerStreamingChangeEventSource spannerStreamingChangeEventSource = new SpannerStreamingChangeEventSource(
                errorHandler, null, eventQueue, metricsEventPublisher, partitionManager,
                schemaRegistryDatabaseClient, spannerEventDispatcher, true, offsetContextFactory);
        Timestamp commitTimestamp = Timestamp.ofTimeMicroseconds(1L);
        ArrayList<Column> rowType = new ArrayList<>();
        io.debezium.connector.spanner.db.model.event.DataChangeEvent dataChangeEvent = spy(new io.debezium.connector.spanner.db.model.event.DataChangeEvent("ABC123",
                commitTimestamp, "42", true, "Record Sequence", "Table Name", rowType, new ArrayList<>(), ModType.INSERT,
                ValueCaptureType.NEW_ROW, 1L, 1L, "Transaction Tag", true, mock(StreamEventMetadata.class)));
        spannerStreamingChangeEventSource.processDataChangeEvent(dataChangeEvent);

        verify(dataChangeEvent).getMods();
    }

    @Test
    void testCommitOffset() {
        SynchronizedPartitionManager partitionManager = spy(new SynchronizedPartitionManager((BlockingConsumer<TaskStateChangeEvent>) mock(BlockingConsumer.class)));

        SpannerStreamingChangeEventSource spannerStreamingChangeEventSource = new SpannerStreamingChangeEventSource(
                null, null, null, null, partitionManager, new SchemaRegistry(
                        "Stream Name", new SchemaDao(mock(DatabaseClient.class)), mock(Runnable.class)),
                null, true, mock(SpannerOffsetContextFactory.class));

        spannerStreamingChangeEventSource.commitOffset(
                Map.of("partitionToken", "v1"), Map.of("offset", Timestamp.now().toString()));

    }

    @Test
    void testCommitRecords() throws InterruptedException {
        SynchronizedPartitionManager partitionManager = new SynchronizedPartitionManager((BlockingConsumer<TaskStateChangeEvent>) mock(BlockingConsumer.class));

        SpannerStreamingChangeEventSource spannerStreamingChangeEventSource = new SpannerStreamingChangeEventSource(
                null, null, null, null, partitionManager, new SchemaRegistry(
                        "Stream Name", new SchemaDao(mock(DatabaseClient.class)), mock(Runnable.class)),
                null, true, mock(SpannerOffsetContextFactory.class));

        SourceRecord sourceRecord1 = spy(new SourceRecord(Map.of(), Map.of(), "t1", Schema.STRING_SCHEMA, "v1"));
        SourceRecord sourceRecord2 = spy(new SourceRecord(Map.of(), Map.of(), "t2", Schema.STRING_SCHEMA, "v2"));
        Map sourcePartition = Map.of("partitionToken", "v1");
        when(sourceRecord2.sourcePartition()).thenReturn(sourcePartition);
        Headers headers = mock(Headers.class);
        Header header = mock(Header.class);
        when(header.value()).thenReturn("header");
        when(headers.lastWithName(anyString())).thenReturn(header);
        when(sourceRecord2.headers()).thenReturn(headers);
        spannerStreamingChangeEventSource.commitRecords(List.of(sourceRecord1, sourceRecord2));

        verify(sourceRecord1, times(2)).sourcePartition();
        verify(sourceRecord1, times(2)).headers();

        verify(sourceRecord2, times(2)).sourcePartition();
        verify(sourceRecord2, times(2)).headers();
    }
}
