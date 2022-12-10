/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor.metadata;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.spanner.context.source.SourceInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * Enables metrics metadata to be extracted
 * from the Spanner event
 *
 */
public class SpannerEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }

        String streamName = sourceInfo.getString(SourceInfo.CHANGE_STREAM_NAME_KEY);
        String tableName = sourceInfo.getString(SourceInfo.TABLE_KEY);

        return Collect.hashMapOf(SourceInfo.SEQUENCE_KEY, sourceInfo.getString(SourceInfo.SEQUENCE_KEY),
                SourceInfo.CHANGE_STREAM_NAME_KEY, streamName,
                SourceInfo.TABLE_KEY, tableName);
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        return sourceInfo.getString(SourceInfo.SERVER_TRANSACTIONAL_ID_KEY);
    }

    @Override
    public String toSummaryString(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        return new EventFormatter()
                .sourcePosition(getEventSourcePosition(source, offset, key, value))
                .key(key)
                .value(value)
                .toString();
    }
}
