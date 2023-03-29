/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ForwardingAsyncResultSet;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;

import io.debezium.connector.spanner.db.model.schema.ChangeStreamSchema;
import io.debezium.connector.spanner.db.model.schema.SpannerSchema;

class SchemaDaoTest {

    @Test
    void testGetSchema() throws SpannerException {
        AsyncResultSet asyncResultSet = mock(AsyncResultSet.class);
        when(asyncResultSet.getBoolean(anyInt())).thenReturn(true);
        when(asyncResultSet.getString(anyInt())).thenReturn("String");
        when(asyncResultSet.getLong(anyInt())).thenReturn(1L);
        when(asyncResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(
                new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(asyncResultSet)))));

        DatabaseClient databaseClient = mock(DatabaseClient.class);
        ReadOnlyTransaction readOnlyTransaction = mock(ReadOnlyTransaction.class);
        when(databaseClient.readOnlyTransaction(any())).thenReturn(readOnlyTransaction);
        when(databaseClient.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString(0)).thenReturn("tableName");
        when(resultSet.getString(1)).thenReturn("columnName");
        when(resultSet.getString(2)).thenReturn("BOOL");
        when(resultSet.getLong(3)).thenReturn(10L);
        when(resultSet.getBoolean(4)).thenReturn(true);
        when(resultSet.getBoolean(5)).thenReturn(true);
        when(resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        when(readOnlyTransaction.executeQuery(any())).thenReturn(resultSet);

        SchemaDao schemaDao = new SchemaDao(databaseClient);
        SpannerSchema schema = schemaDao.getSchema(Timestamp.ofTimeMicroseconds(1L));
        assertFalse(schema.getAllTables().isEmpty());
        assertEquals(1, schema.getAllTables().size());
        assertEquals("tableName", schema.getAllTables().iterator().next().getTableName());
    }

    @Test
    void testGetStream() throws SpannerException {
        AsyncResultSet asyncResultSet = mock(AsyncResultSet.class);
        when(asyncResultSet.getBoolean(anyInt())).thenReturn(true);
        when(asyncResultSet.getString(anyInt())).thenReturn("String");
        when(asyncResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

        ReadOnlyTransaction readOnlyTransaction = mock(ReadOnlyTransaction.class);
        when(readOnlyTransaction.executeQuery(any(), any()))
                .thenReturn(new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(
                        new ForwardingAsyncResultSet(new ForwardingAsyncResultSet(asyncResultSet))))));
        doNothing().when(readOnlyTransaction).close();

        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.readOnlyTransaction(any())).thenReturn(readOnlyTransaction);

        SchemaDao schemaDao = new SchemaDao(databaseClient);
        ChangeStreamSchema actualStream = schemaDao.getStream(Timestamp.ofTimeMicroseconds(1L), "Stream Name");

        assertEquals("Stream Name", actualStream.getName());
        assertTrue(actualStream.isWatchedAllTables());
        verify(databaseClient).readOnlyTransaction(any());
        verify(readOnlyTransaction).executeQuery(any(), any());
        verify(readOnlyTransaction).close();
        verify(asyncResultSet, atLeast(1)).next();
        verify(asyncResultSet, atLeast(1)).getBoolean(anyInt());
    }
}