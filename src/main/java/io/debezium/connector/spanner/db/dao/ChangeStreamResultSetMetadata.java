/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import org.joda.time.Duration;

import com.google.cloud.Timestamp;

/**
 * Metadata for the Spanner record
 */
public class ChangeStreamResultSetMetadata {
    private final Timestamp queryStartedAt;
    private final Timestamp recordStreamStartedAt;
    private final Timestamp recordStreamEndedAt;
    private final Timestamp recordReadAt;
    private final Duration totalStreamDuration;
    private final long numberOfRecordsRead;

    ChangeStreamResultSetMetadata(
                                  Timestamp queryStartedAt,
                                  Timestamp recordStreamStartedAt,
                                  Timestamp recordStreamEndedAt,
                                  Timestamp recordReadAt,
                                  Duration totalStreamDuration,
                                  long numberOfRecordsRead) {
        this.queryStartedAt = queryStartedAt;
        this.recordStreamStartedAt = recordStreamStartedAt;
        this.recordStreamEndedAt = recordStreamEndedAt;
        this.recordReadAt = recordReadAt;
        this.totalStreamDuration = totalStreamDuration;
        this.numberOfRecordsRead = numberOfRecordsRead;
    }

    public Timestamp getQueryStartedAt() {
        return queryStartedAt;
    }

    public Timestamp getRecordStreamStartedAt() {
        return recordStreamStartedAt;
    }

    public Timestamp getRecordStreamEndedAt() {
        return recordStreamEndedAt;
    }

    public Timestamp getRecordReadAt() {
        return recordReadAt;
    }

    public Duration getTotalStreamDuration() {
        return totalStreamDuration;
    }

    public long getNumberOfRecordsRead() {
        return numberOfRecordsRead;
    }
}
