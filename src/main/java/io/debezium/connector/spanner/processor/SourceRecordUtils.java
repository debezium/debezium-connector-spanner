/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.spanner.SpannerPartition;

public class SourceRecordUtils {

    private static final String RECORD_UID = "spannerDataChangeRecordUid";

    private static final String EMIT_AT_TIMESTAMP_KEY = "emitAtTimestamp";

    private static final String PUBLISH_AT_TIMESTAMP_KEY = "publishAtTimestamp";

    private static final String POLL_AT_TIMESTAMP_KEY = "pollAtTimestamp";

    private SourceRecordUtils() {
    }

    public static boolean isDataChangeRecord(SourceRecord sourceRecord) {
        return extractRecordUid(sourceRecord) != null;
    }

    public static ConnectHeaders from(String recordUid) {
        ConnectHeaders connectHeaders = new ConnectHeaders();
        connectHeaders.addString(RECORD_UID, recordUid);
        return connectHeaders;
    }

    public static SourceRecord addEmitTimestamp(SourceRecord sourceRecord, long timestamp) {
        return withTimestampHeader(sourceRecord, EMIT_AT_TIMESTAMP_KEY, timestamp);
    }

    public static SourceRecord addPublishTimestamp(SourceRecord sourceRecord, long timestamp) {
        return withTimestampHeader(sourceRecord, PUBLISH_AT_TIMESTAMP_KEY, timestamp);
    }

    public static SourceRecord addPollTimestamp(SourceRecord sourceRecord, long timestamp) {
        return withTimestampHeader(sourceRecord, POLL_AT_TIMESTAMP_KEY, timestamp);
    }

    /**
     * Returns a copy of the record with the header added, instead of changing the record's headers in place.
     * The headers of a record can be read by another thread at the same time (Kafka Connect duplicates them in
     * the transformation chain), and a record and its tombstone can share the same headers.
     */
    private static SourceRecord withTimestampHeader(SourceRecord sourceRecord, String key, long timestamp) {
        Headers headers = sourceRecord.headers().duplicate();
        headers.addLong(key, timestamp);
        return sourceRecord.newRecord(sourceRecord.topic(), sourceRecord.kafkaPartition(), sourceRecord.keySchema(),
                sourceRecord.key(), sourceRecord.valueSchema(), sourceRecord.value(), sourceRecord.timestamp(), headers);
    }

    public static String extractToken(SourceRecord sourceRecord) {
        if (sourceRecord.sourcePartition() == null) {
            return null;
        }
        return SpannerPartition.extractToken(sourceRecord.sourcePartition());
    }

    public static String extractTvfName(SourceRecord sourceRecord) {
        if (sourceRecord.sourcePartition() == null) {
            return null;
        }
        return SpannerPartition.extractTvfName(sourceRecord.sourcePartition());
    }

    public static String extractRecordUid(SourceRecord sourceRecord) {
        return (String) extract(RECORD_UID, sourceRecord);
    }

    public static Long extractEmitTimestamp(SourceRecord sourceRecord) {
        return (Long) extract(EMIT_AT_TIMESTAMP_KEY, sourceRecord);
    }

    public static Long extractPublishTimestamp(SourceRecord sourceRecord) {
        return (Long) extract(PUBLISH_AT_TIMESTAMP_KEY, sourceRecord);
    }

    public static Long extractPollTimestamp(SourceRecord sourceRecord) {
        return (Long) extract(POLL_AT_TIMESTAMP_KEY, sourceRecord);
    }

    private static Object extract(String headerName, SourceRecord sourceRecord) {
        if (sourceRecord.headers() == null) {
            return null;
        }

        Header header = sourceRecord.headers().lastWithName(headerName);
        if (header != null) {
            return header.value();
        }
        return null;
    }
}
