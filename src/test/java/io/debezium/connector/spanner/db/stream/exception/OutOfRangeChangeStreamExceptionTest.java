/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream.exception;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.util.HashSet;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.SpannerException;

import io.debezium.connector.spanner.db.model.Partition;

class OutOfRangeChangeStreamExceptionTest {

    @Test
    void testConstructor() {
        HashSet<String> parentTokens = new HashSet<>();
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(1L);
        Partition partition = new Partition("partitionToken", parentTokens, startTimestamp, Timestamp.ofTimeMicroseconds(1L), "originParent");

        assertSame(partition,
                new OutOfRangeChangeStreamException(partition, mock(SpannerException.class)).getPartition());
    }
}
