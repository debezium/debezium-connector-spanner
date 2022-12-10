/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.schema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;

/**
 * Kafka record schema for Spanner DB tables
 */
public class KafkaSpannerSchema implements DatabaseSchema<TableId> {
    private final KafkaSpannerTableSchemaFactory tableSchemaFactory;

    private final Map<TableId, KafkaSpannerTableSchema> cache = new ConcurrentHashMap<>();

    public KafkaSpannerSchema(KafkaSpannerTableSchemaFactory tableSchemaFactory) {
        this.tableSchemaFactory = tableSchemaFactory;
    }

    @Override
    public DataCollectionSchema schemaFor(TableId tableId) {
        return cache.computeIfAbsent(tableId, tableSchemaFactory::getTableSchema);
    }

    @Override
    public boolean tableInformationComplete() {
        return false;
    }

    @Override
    public boolean isHistorized() {
        return false;
    }

    @Override
    public void close() {
    }

    public void resetCache() {
        cache.clear();
    }
}
