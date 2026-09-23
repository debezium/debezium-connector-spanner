/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ForwardingAsyncResultSet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.Statement;

class ChangeStreamDaoTest {

    @Test
    void testStreamQuery() {
        ReadContext readContext = mock(ReadContext.class);
        when(readContext.executeQuery(any(), any(), any()))
                .thenReturn(new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(
                        new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(mock(AsyncResultSet.class)))))));

        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.singleUse()).thenReturn(readContext);

        ChangeStreamDao changeStreamDao = new ChangeStreamDao("Change Stream Name", false, databaseClient,
                Options.RpcPriority.LOW, "Job Name");
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(1L);
        assertNull(changeStreamDao.streamQuery("token", startTimestamp, Timestamp.ofTimeMicroseconds(1L), 1L)
                .getCurrentRowAsStruct());

        verify(databaseClient).singleUse();
        verify(readContext).executeQuery(any(), any(), any());
    }

    @Test
    void testPostgresStreamQueryUsesPlacementTvfName() {
        ReadContext readContext = readContext();
        DatabaseClient databaseClient = postgresDatabaseClient(readContext);
        ChangeStreamDao changeStreamDao = new ChangeStreamDao("ChangeStream", false,
                List.of("read_json_changestream_us"), databaseClient, Options.RpcPriority.LOW, "Job Name");

        changeStreamDao.streamQuery("token", "read_json_changestream_us",
                Timestamp.ofTimeMicroseconds(1L), Timestamp.ofTimeMicroseconds(2L), 1L);

        assertEquals("SELECT * FROM \"spanner\".\"read_json_changestream_us\"($1, $2, $3, $4, null)",
                executedStatement(readContext).getSql());
    }

    @Test
    void testPostgresStreamQueryFoldsUnquotedPlacementTvfNameToLowercase() {
        ReadContext readContext = readContext();
        DatabaseClient databaseClient = postgresDatabaseClient(readContext);
        ChangeStreamDao changeStreamDao = new ChangeStreamDao("ChangeStream", true,
                List.of("READ_PROTO_BYTES_CHANGESTREAM_US"), databaseClient, Options.RpcPriority.LOW, "Job Name");

        changeStreamDao.streamQuery("token", "READ_PROTO_BYTES_CHANGESTREAM_US",
                Timestamp.ofTimeMicroseconds(1L), Timestamp.ofTimeMicroseconds(2L), 1L);

        assertEquals("SELECT * FROM \"spanner\".\"read_proto_bytes_changestream_us\"($1, $2, $3, $4, null)",
                executedStatement(readContext).getSql());
    }

    @Test
    void testPostgresStreamQueryPreservesQuotedPlacementTvfNameCase() {
        ReadContext readContext = readContext();
        DatabaseClient databaseClient = postgresDatabaseClient(readContext);
        ChangeStreamDao changeStreamDao = new ChangeStreamDao("ChangeStream", true,
                List.of("\"spanner\".\"Read_Proto_Bytes_ChangeStream_US\""), databaseClient, Options.RpcPriority.LOW, "Job Name");

        changeStreamDao.streamQuery("token", "\"spanner\".\"Read_Proto_Bytes_ChangeStream_US\"",
                Timestamp.ofTimeMicroseconds(1L), Timestamp.ofTimeMicroseconds(2L), 1L);

        assertEquals("SELECT * FROM \"spanner\".\"Read_Proto_Bytes_ChangeStream_US\"($1, $2, $3, $4, null)",
                executedStatement(readContext).getSql());
    }

    @Test
    void testPostgresStreamQueryAcceptsSchemaQualifiedPlacementTvfName() {
        ReadContext readContext = readContext();
        DatabaseClient databaseClient = postgresDatabaseClient(readContext);
        ChangeStreamDao changeStreamDao = new ChangeStreamDao("ChangeStream", true,
                List.of("\"spanner\".\"read_proto_bytes_changestream_us\""), databaseClient, Options.RpcPriority.LOW, "Job Name");

        changeStreamDao.streamQuery("token", "\"spanner\".\"read_proto_bytes_changestream_us\"",
                Timestamp.ofTimeMicroseconds(1L), Timestamp.ofTimeMicroseconds(2L), 1L);

        assertEquals("SELECT * FROM \"spanner\".\"read_proto_bytes_changestream_us\"($1, $2, $3, $4, null)",
                executedStatement(readContext).getSql());
    }

    @Test
    void testPostgresStreamQueryFallsBackToDefaultTvfName() {
        ReadContext readContext = readContext();
        DatabaseClient databaseClient = postgresDatabaseClient(readContext);
        ChangeStreamDao changeStreamDao = new ChangeStreamDao("ChangeStream", false, databaseClient,
                Options.RpcPriority.LOW, "Job Name");

        changeStreamDao.streamQuery("token", null,
                Timestamp.ofTimeMicroseconds(1L), Timestamp.ofTimeMicroseconds(2L), 1L);

        assertEquals("SELECT * FROM \"spanner\".\"read_json_changestream\"($1, $2, $3, $4, null)",
                executedStatement(readContext).getSql());
    }

    private static ReadContext readContext() {
        ReadContext readContext = mock(ReadContext.class);
        when(readContext.executeQuery(any(), any(), any()))
                .thenReturn(new ForwardingAsyncResultSet(mock(AsyncResultSet.class)));
        return readContext;
    }

    private static DatabaseClient postgresDatabaseClient(ReadContext readContext) {
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.getDialect()).thenReturn(Dialect.POSTGRESQL);
        when(databaseClient.singleUse()).thenReturn(readContext);
        return databaseClient;
    }

    private static Statement executedStatement(ReadContext readContext) {
        ArgumentCaptor<Statement> statementCaptor = ArgumentCaptor.forClass(Statement.class);
        verify(readContext).executeQuery(statementCaptor.capture(), any(), any());
        return statementCaptor.getValue();
    }
}
