/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.connector.spanner.context.source.SourceInfoFactory;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.exception.SpannerConnectorException;
import io.debezium.connector.spanner.kafka.KafkaPartitionInfoProvider;
import io.debezium.data.Envelope;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Spanner dispatcher for data change and schema change events.
 */
public class SpannerEventDispatcher extends EventDispatcher<SpannerPartition, TableId> {

    private static final Logger LOGGER = getLogger(SpannerEventDispatcher.class);

    private final SpannerConnectorConfig connectorConfig;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final TopicNamingStrategy<TableId> topicNamingStrategy;
    private final SchemaRegistry schemaRegistry;

    private final DatabaseSchema<TableId> schema;

    private final SourceInfoFactory sourceInfoFactory;

    private final KafkaPartitionInfoProvider kafkaPartitionInfoProvider;

    public SpannerEventDispatcher(SpannerConnectorConfig connectorConfig,
                                  TopicNamingStrategy<TableId> topicNamingStrategy,
                                  DatabaseSchema<TableId> schema,
                                  ChangeEventQueue<DataChangeEvent> queue,
                                  DataCollectionFilters.DataCollectionFilter<TableId> filter,
                                  ChangeEventCreator changeEventCreator,
                                  EventMetadataProvider metadataProvider,
                                  HeartbeatFactory<TableId> heartbeatFactory,
                                  SchemaNameAdjuster schemaNameAdjuster,
                                  SchemaRegistry schemaRegistry,
                                  SourceInfoFactory sourceInfoFactory,
                                  KafkaPartitionInfoProvider kafkaPartitionInfoProvider) {
        super(connectorConfig, topicNamingStrategy, schema, queue, filter, changeEventCreator, metadataProvider,
                heartbeatFactory.createHeartbeat(), schemaNameAdjuster);
        this.connectorConfig = connectorConfig;
        this.queue = queue;
        this.topicNamingStrategy = topicNamingStrategy;
        this.schemaRegistry = schemaRegistry;
        this.schema = schema;
        this.sourceInfoFactory = sourceInfoFactory;
        this.kafkaPartitionInfoProvider = kafkaPartitionInfoProvider;
    }

    public boolean publishLowWatermarkStampEvent() {

        try {
            for (TableId tableId : schemaRegistry.getAllTables()) {

                String topicName = topicNamingStrategy.dataChangeTopic(tableId);

                DataCollectionSchema dataCollectionSchema = schema.schemaFor(tableId);

                Struct sourceStruct = sourceInfoFactory.getSourceInfoForLowWatermarkStamp(tableId).struct();

                int numPartitions = connectorConfig.getTopicNumPartitions();
                for (int partition : kafkaPartitionInfoProvider.getPartitions(topicName, Optional.of(numPartitions))) {
                    SourceRecord sourceRecord = emitSourceRecord(topicName, dataCollectionSchema, partition, sourceStruct);
                    LOGGER.debug("Build low watermark stamp record {} ", sourceRecord);

                    queue.enqueue(new DataChangeEvent(sourceRecord));
                }
            }

            return true;

        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
        catch (Exception ex) {
            if (CommonConnectorConfig.EventProcessingFailureHandlingMode.FAIL
                    .equals(connectorConfig.getEventProcessingFailureHandlingMode())) {
                throw new SpannerConnectorException("Error while publishing watermark stamp", ex);
            }
            LOGGER.warn("Error while publishing watermark stamp");

            return false;
        }
    }

    @VisibleForTesting
    SourceRecord emitSourceRecord(String topicName, DataCollectionSchema dataCollectionSchema,
                                  int partition, Struct sourceStruct) {

        Struct envelope = buildMessage(dataCollectionSchema.getEnvelopeSchema(), sourceStruct);

        return new SourceRecord(null,
                null,
                topicName,
                partition,
                null,
                null,
                dataCollectionSchema.getEnvelopeSchema().schema(),
                envelope,
                null,
                SourceRecordUtils.from("watermark-" + UUID.randomUUID()));
    }

    @VisibleForTesting
    Struct buildMessage(Envelope envelope, Struct sourceStruct) {
        Struct struct = new Struct(envelope.schema());
        struct.put(Envelope.FieldName.OPERATION, Envelope.Operation.MESSAGE.code());
        struct.put(Envelope.FieldName.SOURCE, sourceStruct);
        struct.put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());
        return struct;
    }

    public void destroy() {
        super.close();
    }

    @Override
    public void close() {
    }

}
