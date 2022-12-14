/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import org.joda.time.Duration;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;

/**
 * Wrapper on top of Spanner result set,
 * which provides additional info
 */
public class ChangeStreamResultSet implements AutoCloseable {

    private final ResultSet resultSet;
    private Timestamp queryStartedAt;
    private Timestamp recordStreamStartedAt;
    private Timestamp recordStreamEndedAt;
    private Timestamp recordReadAt;
    private Duration totalStreamDuration;
    private long numberOfRecordsRead;

    ChangeStreamResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
        this.queryStartedAt = Timestamp.MIN_VALUE;
        this.recordStreamStartedAt = Timestamp.MIN_VALUE;
        this.recordStreamEndedAt = Timestamp.MIN_VALUE;
        this.recordReadAt = Timestamp.MIN_VALUE;
        this.totalStreamDuration = Duration.ZERO;
        this.numberOfRecordsRead = 0L;
    }

    public boolean next() {
        if (queryStartedAt == null) {
            queryStartedAt = Timestamp.now();
        }
        recordStreamStartedAt = Timestamp.now();
        final boolean hasNext = resultSet.next();
        numberOfRecordsRead++;
        recordStreamEndedAt = Timestamp.now();
        totalStreamDuration = totalStreamDuration.withDurationAdded(
                new Duration(
                        recordStreamStartedAt.toSqlTimestamp().getTime(),
                        recordStreamEndedAt.toSqlTimestamp().getTime()),
                1);
        return hasNext;
    }

    public Struct getCurrentRowAsStruct() {
        recordReadAt = Timestamp.now();
        return resultSet.getCurrentRowAsStruct();
    }

    public ChangeStreamResultSetMetadata getMetadata() {
        return new ChangeStreamResultSetMetadata(
                queryStartedAt,
                recordStreamStartedAt,
                recordStreamEndedAt,
                recordReadAt,
                totalStreamDuration,
                numberOfRecordsRead);
    }

    @Override
    public void close() {
        resultSet.close();
    }
}
