/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor.heartbeat;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * Generates Spanner Heartbeat messages
 */
public class SpannerHeartbeat implements Heartbeat {
    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerHeartbeat.class);

    private static final String PARTITION_TOKEN_KEY = "partitionToken";

    private final String topicName;

    private final Schema keySchema;
    private final Schema valueSchema;

    public SpannerHeartbeat(String topicName, SchemaNameAdjuster schemaNameAdjuster) {
        this.topicName = topicName;

        keySchema = SchemaBuilder.struct()
                .name(schemaNameAdjuster.adjust("io.debezium.connector.spanner.processor.heartbeat.PartitionTokenKey"))
                .field(PARTITION_TOKEN_KEY, Schema.STRING_SCHEMA)
                .build();

        valueSchema = SchemaBuilder.struct()
                .name(schemaNameAdjuster.adjust("io.debezium.connector.spanner.processor.heartbeat.SpannerHeartbeat"))
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();
    }

    @Override
    public void heartbeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        forcedBeat(partition, offset, consumer);
    }

    @Override
    public void heartbeat(Map<String, ?> partition, OffsetProducer offsetProducer, BlockingConsumer<SourceRecord> consumer) throws InterruptedException {
        forcedBeat(partition, offsetProducer.offset(), consumer);
    }

    @Override
    public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer)
            throws InterruptedException {
        LOGGER.debug("Generating heartbeat event");
        consumer.accept(heartbeatRecord((Map<String, String>) partition, offset));
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @VisibleForTesting
    Struct partitionTokenKey(String partitionToken) {
        Struct result = new Struct(keySchema);
        result.put(PARTITION_TOKEN_KEY, partitionToken);
        return result;
    }

    @VisibleForTesting
    Struct messageValue() {
        Struct result = new Struct(valueSchema);
        result.put(AbstractSourceInfo.TIMESTAMP_KEY, Instant.now().toEpochMilli());
        return result;
    }

    @VisibleForTesting
    SourceRecord heartbeatRecord(Map<String, String> sourcePartition, Map<String, ?> sourceOffset) {
        String token = SpannerPartition.extractToken(sourcePartition);
        return new SourceRecord(sourcePartition, sourceOffset, topicName, 0,
                keySchema, partitionTokenKey(token), valueSchema, messageValue());
    }

}
