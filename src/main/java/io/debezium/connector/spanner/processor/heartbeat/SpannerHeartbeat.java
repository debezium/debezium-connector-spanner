/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor.heartbeat;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * Generates Spanner Heartbeat messages
 */
public class SpannerHeartbeat implements ScheduledHeartbeat {
    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerHeartbeat.class);

    private static final String PARTITION_TOKEN_KEY = "partitionToken";

    private final String topicName;

    private final Schema keySchema;
    private final Schema valueSchema;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final Clock clock;

    public SpannerHeartbeat(String topicName,
                            SchemaNameAdjuster schemaNameAdjuster,
                            ChangeEventQueue<DataChangeEvent> queue,
                            Clock clock) {
        this.topicName = topicName;

        keySchema = SchemaBuilder.struct()
                .name(schemaNameAdjuster.adjust("io.debezium.connector.spanner.processor.heartbeat.PartitionTokenKey"))
                .field(PARTITION_TOKEN_KEY, Schema.STRING_SCHEMA)
                .build();

        valueSchema = SchemaBuilder.struct()
                .name(schemaNameAdjuster.adjust("io.debezium.connector.spanner.processor.heartbeat.SpannerHeartbeat"))
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();
        this.queue = queue;
        this.clock = clock;
    }

    @Override
    public void emitWithDelay(Map<String, ?> partition, OffsetContext offset) throws InterruptedException {
        this.emit(partition, offset);
    }

    @Override
    public void emit(Map<String, ?> partition, OffsetContext offset) throws InterruptedException {
        LOGGER.debug("Generating heartbeat event");
        this.queue.enqueue(new DataChangeEvent(heartbeatRecord((Map<String, String>) partition, offset.getOffset())));
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    private Struct partitionTokenKey(String partitionToken) {
        Struct result = new Struct(keySchema);
        result.put(PARTITION_TOKEN_KEY, partitionToken);
        return result;
    }

    private Struct messageValue() {
        Struct result = new Struct(valueSchema);
        result.put(AbstractSourceInfo.TIMESTAMP_KEY, Instant.now(clock).toEpochMilli());
        return result;
    }

    private SourceRecord heartbeatRecord(Map<String, String> sourcePartition, Map<String, ?> sourceOffset) {
        String token = SpannerPartition.extractToken(sourcePartition);
        return new SourceRecord(sourcePartition, sourceOffset, topicName, 0,
                keySchema, partitionTokenKey(token), valueSchema, messageValue());
    }

}
