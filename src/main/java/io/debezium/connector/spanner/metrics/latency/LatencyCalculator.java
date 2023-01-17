/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.latency;

import java.time.Instant;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.context.source.SourceInfo;
import io.debezium.connector.spanner.processor.SourceRecordUtils;

/**
 * Utility to calculate various of connector latencies
 */
public class LatencyCalculator {

    private LatencyCalculator() {
    }

    public static Long getTotalLatency(SourceRecord record) {
        Struct source = getSource(record);
        if (source == null) {
            return null;
        }

        long commitTimestamp = source.getInt64(SourceInfo.TIMESTAMP_KEY);

        long pollAtTimestamp = SourceRecordUtils.extractPollTimestamp(record);

        return pollAtTimestamp - commitTimestamp;
    }

    public static Long getReadToEmitLatency(SourceRecord record) {
        Struct source = getSource(record);
        if (source == null) {
            return null;
        }

        Long readAtTimestamp = source.getInt64(SourceInfo.READ_AT_TIMESTAMP_KEY);
        if (readAtTimestamp == null) {
            return null;
        }

        long pollAtTimestamp = SourceRecordUtils.extractPollTimestamp(record);

        return pollAtTimestamp - readAtTimestamp;
    }

    public static Long getSpannerLatency(SourceRecord record) {
        Struct source = getSource(record);
        if (source == null) {
            return null;
        }

        Long readAtTimestamp = source.getInt64(SourceInfo.READ_AT_TIMESTAMP_KEY);
        if (readAtTimestamp == null) {
            return null;
        }

        long commitTimestamp = source.getInt64(SourceInfo.TIMESTAMP_KEY);

        return readAtTimestamp - commitTimestamp;
    }

    public static Long getCommitToEmitLatency(SourceRecord record) {
        Struct source = getSource(record);
        if (source == null) {
            return null;
        }

        long commitTimestamp = source.getInt64(SourceInfo.TIMESTAMP_KEY);

        long pollAtTimestamp = SourceRecordUtils.extractPollTimestamp(record);

        return pollAtTimestamp - commitTimestamp;
    }

    public static Long getTimeBehindLowWatermark(Timestamp lowWatermark) {
        return Instant.now().toEpochMilli() - lowWatermark.toSqlTimestamp().toInstant().toEpochMilli();
    }

    public static Long getCommitToPublishLatency(SourceRecord record) {
        Struct source = getSource(record);

        Long publishAtTimestamp = SourceRecordUtils.extractPublishTimestamp(record);

        if (source == null || publishAtTimestamp == null) {
            return null;
        }

        long commitAtTimestamp = source.getInt64(SourceInfo.TIMESTAMP_KEY);

        return publishAtTimestamp - commitAtTimestamp;
    }

    public static Long getEmitToPublishLatency(SourceRecord record) {
        Struct source = getSource(record);

        Long publishAtTimestamp = SourceRecordUtils.extractPublishTimestamp(record);

        if (source == null || publishAtTimestamp == null) {
            return null;
        }
        long pollAtTimestamp = SourceRecordUtils.extractPollTimestamp(record);

        return publishAtTimestamp - pollAtTimestamp;
    }

    public static Long getOwnConnectorLatency(SourceRecord record) {
        Struct source = getSource(record);
        if (source == null) {
            return null;
        }

        Long readAtTimestamp = source.getInt64(SourceInfo.READ_AT_TIMESTAMP_KEY);
        if (readAtTimestamp == null) {
            return null;
        }

        Long emitAtTimestamp = SourceRecordUtils.extractEmitTimestamp(record);
        if (emitAtTimestamp == null) {
            return null;
        }

        return emitAtTimestamp - readAtTimestamp;
    }

    public static Long getLowWatermarkLag(SourceRecord record) {
        Struct source = getSource(record);
        if (source == null) {
            return null;
        }

        Long lowWatermark = source.getInt64(SourceInfo.LOW_WATERMARK_KEY);

        if (lowWatermark != null) {
            return Instant.now().toEpochMilli() - lowWatermark;
        }
        return null;
    }

    private static Struct getSource(SourceRecord record) {
        Object value = record.value();
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            if (struct.schema().field(SourceInfo.SOURCE_KEY) == null) {
                return null;
            }
            return struct.getStruct(SourceInfo.SOURCE_KEY);
        }
        return null;
    }
}
