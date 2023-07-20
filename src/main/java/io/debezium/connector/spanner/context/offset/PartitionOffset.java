/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.context.offset;

import java.util.Map;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.StreamEventMetadata;

/**
 * Stores offset, makes offset map for kafka connect
 */
public class PartitionOffset {

    private static final String OFFSET_KEY = "offset";
    private static final String DEBUG_START_TIME_KEY = "startTime";
    private static final String DEBUG_QUERY_STARTED_AT_KEY = "queryStartedAt";

    private final Timestamp offset;

    private final StreamEventMetadata metadata;

    public PartitionOffset(Timestamp offset, StreamEventMetadata metadata) {
        this.offset = offset;
        this.metadata = metadata;
    }

    public PartitionOffset() {
        this.metadata = null;
        this.offset = null;
    }

    public Map<String, String> getOffset() {
        if (this.offset == null) {
            return null;
        }
        return Map.of(OFFSET_KEY, offset.toString(),
                DEBUG_START_TIME_KEY, metadata.getPartitionStartTimestamp().toString(),
                DEBUG_QUERY_STARTED_AT_KEY, metadata.getQueryStartedAt() != null ? metadata.getQueryStartedAt().toString() : "");
    }

    public static Timestamp extractOffset(Map<String, ?> offsets) {
        if (offsets == null) {
            return null;
        }
        String offset = (String) offsets.get(OFFSET_KEY);
        return offset == null ? null : Timestamp.parseTimestamp(offset);
    }

}
