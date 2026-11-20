/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ForwardingAsyncResultSet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;

import io.debezium.connector.spanner.db.model.InitialPartition;

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

    @Test
    void testIsExternalPlacementTokenRejectedByAllConfiguredTvfsAndCached() {
        AsyncResultSet rs = mock(AsyncResultSet.class);
        when(rs.next()).thenThrow(SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                "Partition token ext-token does not belong to the placement TVF"));
        ReadContext readContext = mock(ReadContext.class);
        when(readContext.executeQuery(any(), any(), any())).thenReturn(new ForwardingAsyncResultSet(rs));
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.singleUse()).thenReturn(readContext);

        ChangeStreamDao changeStreamDao = new ChangeStreamDao("ChangeStream", true,
                List.of("READ_cs_p1", "READ_cs_p2"), databaseClient, Options.RpcPriority.LOW, "Job Name");

        Timestamp probeTs = Timestamp.ofTimeMicroseconds(100L);
        assertTrue(changeStreamDao.isExternalPlacementToken("ext-token", probeTs));
        // Second call must hit cache without issuing additional queries
        assertTrue(changeStreamDao.isExternalPlacementToken("ext-token", probeTs));
        verify(readContext, times(2)).executeQuery(any(), any(), any());
    }

    @Test
    void testIsExternalPlacementTokenAcceptedByConfiguredTvfReturnsFalseAndCaches() {
        AsyncResultSet rs1 = mock(AsyncResultSet.class);
        when(rs1.next()).thenThrow(SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                "Partition token p2-token does not belong to the placement TVF"));
        AsyncResultSet rs2 = mock(AsyncResultSet.class);
        when(rs2.next()).thenReturn(false);

        ReadContext readContext = mock(ReadContext.class);
        when(readContext.executeQuery(any(), any(), any()))
                .thenReturn(new ForwardingAsyncResultSet(rs1))
                .thenReturn(new ForwardingAsyncResultSet(rs2));
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.singleUse()).thenReturn(readContext);

        ChangeStreamDao changeStreamDao = new ChangeStreamDao("ChangeStream", true,
                List.of("READ_cs_p1", "READ_cs_p2"), databaseClient, Options.RpcPriority.LOW, "Job Name");

        Timestamp probeTs = Timestamp.ofTimeMicroseconds(100L);
        assertFalse(changeStreamDao.isExternalPlacementToken("p2-token", probeTs));
        // Second call must hit cache without issuing additional queries
        assertFalse(changeStreamDao.isExternalPlacementToken("p2-token", probeTs));
        verify(readContext, times(2)).executeQuery(any(), any(), any());
    }

    @Test
    void testIsExternalPlacementTokenTransientErrorReturnsFalseWithoutCaching() {
        AsyncResultSet rs1 = mock(AsyncResultSet.class);
        when(rs1.next()).thenThrow(SpannerExceptionFactory.newSpannerException(
                ErrorCode.UNAVAILABLE, "Transient network error"));
        AsyncResultSet rs2 = mock(AsyncResultSet.class);
        when(rs2.next()).thenThrow(SpannerExceptionFactory.newSpannerException(
                ErrorCode.INVALID_ARGUMENT,
                "Partition token ext-token does not belong to the placement TVF"));

        ReadContext readContext = mock(ReadContext.class);
        when(readContext.executeQuery(any(), any(), any()))
                .thenReturn(new ForwardingAsyncResultSet(rs1))
                .thenReturn(new ForwardingAsyncResultSet(rs2));
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.singleUse()).thenReturn(readContext);

        ChangeStreamDao changeStreamDao = new ChangeStreamDao("ChangeStream", true,
                List.of("READ_cs_p1"), databaseClient, Options.RpcPriority.LOW, "Job Name");

        Timestamp probeTs = Timestamp.ofTimeMicroseconds(100L);
        assertFalse(changeStreamDao.isExternalPlacementToken("ext-token", probeTs));
        // Because transient error was not cached, retry probes again and succeeds
        assertTrue(changeStreamDao.isExternalPlacementToken("ext-token", probeTs));
        assertFalse(changeStreamDao.isExternalPlacementToken(InitialPartition.PARTITION_TOKEN, probeTs));
        verify(readContext, times(2)).executeQuery(any(), any(), any());
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
