/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ForwardingAsyncResultSet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ReadContext;

class ChangeStreamDaoTest {

    @Test
    void testStreamQuery() {
        ReadContext readContext = mock(ReadContext.class);
        when(readContext.executeQuery(any(), any()))
                .thenReturn(new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(
                        new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(mock(AsyncResultSet.class)))))));

        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.singleUse()).thenReturn(readContext);

        ChangeStreamDao changeStreamDao = new ChangeStreamDao("Change Stream Name", databaseClient,
                Options.RpcPriority.LOW, "Job Name");
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(1L);
        assertNull(changeStreamDao.streamQuery("token", startTimestamp, Timestamp.ofTimeMicroseconds(1L), 1L)
                .getCurrentRowAsStruct());

        verify(databaseClient).singleUse();
        verify(readContext).executeQuery(any(), any());
    }
}
