/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

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
        when(readOnlyTransaction.executeQuery(any()))
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
        verify(readOnlyTransaction).executeQuery(any());
        verify(readOnlyTransaction).close();
        verify(asyncResultSet, atLeast(1)).next();
        verify(asyncResultSet, atLeast(1)).getBoolean(anyInt());
    }

    @Test
    void testIsMutableKeyRangeChangeStream() throws SpannerException {
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        ReadOnlyTransaction readOnlyTransaction = mock(ReadOnlyTransaction.class);
        when(databaseClient.readOnlyTransaction()).thenReturn(readOnlyTransaction);

        ResultSet resultSet = mock(ResultSet.class);
        when(readOnlyTransaction.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getString(0)).thenReturn("partition_mode");
        when(resultSet.getString(1)).thenReturn("MUTABLE_KEY_RANGE");

        SchemaDao schemaDao = new SchemaDao(databaseClient);
        boolean actual = schemaDao.isMutableKeyRangeChangeStream("someStream");
        assertTrue(actual);

        verify(databaseClient).readOnlyTransaction();
        verify(readOnlyTransaction).executeQuery(any());
    }

    @Test
    void testIsNotMutableKeyRangeChangeStream() throws SpannerException {
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        ReadOnlyTransaction readOnlyTransaction = mock(ReadOnlyTransaction.class);
        when(databaseClient.readOnlyTransaction()).thenReturn(readOnlyTransaction);

        ResultSet resultSet = mock(ResultSet.class);
        when(readOnlyTransaction.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getString(0)).thenReturn("some_other_option");
        when(resultSet.getString(1)).thenReturn("SOME_VALUE");

        SchemaDao schemaDao = new SchemaDao(databaseClient);
        boolean actual = schemaDao.isMutableKeyRangeChangeStream("someStream");
        assertFalse(actual);

        verify(databaseClient).readOnlyTransaction();
        verify(readOnlyTransaction).executeQuery(any());
    }

    @Test
    void testValidatePlacementTvfNamesEmptyListIsNoOp() throws SpannerException {
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        SchemaDao schemaDao = new SchemaDao(databaseClient);

        assertDoesNotThrow(() -> schemaDao.validatePlacementTvfNames("Foo", List.of()));

        verify(databaseClient, org.mockito.Mockito.never()).readOnlyTransaction();
    }

    @Test
    void testValidatePlacementTvfNamesGoogleSqlSuccess() throws SpannerException {
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
        ReadOnlyTransaction readOnlyTransaction = mock(ReadOnlyTransaction.class);
        when(databaseClient.readOnlyTransaction()).thenReturn(readOnlyTransaction);

        ResultSet optionsResultSet = mock(ResultSet.class);
        when(optionsResultSet.next()).thenReturn(true, false);
        when(optionsResultSet.getString(0)).thenReturn("per_placement_tvf");
        when(optionsResultSet.getString(1)).thenReturn("true");

        ResultSet routinesResultSet = mock(ResultSet.class);
        when(routinesResultSet.next()).thenReturn(true, true, false);
        when(routinesResultSet.getString(0)).thenReturn("READ_Foo_US", "READ_Foo_EU");

        when(readOnlyTransaction.executeQuery(any())).thenReturn(optionsResultSet, routinesResultSet);

        SchemaDao schemaDao = new SchemaDao(databaseClient);
        assertDoesNotThrow(() -> schemaDao.validatePlacementTvfNames("Foo", List.of("READ_Foo_US", "READ_Foo_EU")));
    }

    @Test
    void testValidatePlacementTvfNamesPostgresFoldsUnquotedNameToLowercase() throws SpannerException {
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.getDialect()).thenReturn(Dialect.POSTGRESQL);
        ReadOnlyTransaction readOnlyTransaction = mock(ReadOnlyTransaction.class);
        when(databaseClient.readOnlyTransaction()).thenReturn(readOnlyTransaction);

        ResultSet optionsResultSet = mock(ResultSet.class);
        when(optionsResultSet.next()).thenReturn(true, true, false);
        when(optionsResultSet.getString(0)).thenReturn("per_placement_tvf", "partition_mode");
        when(optionsResultSet.getString(1)).thenReturn("true", "MUTABLE_KEY_RANGE");

        ResultSet routinesResultSet = mock(ResultSet.class);
        when(routinesResultSet.next()).thenReturn(true, false);
        when(routinesResultSet.getString(0)).thenReturn("read_proto_bytes_foo_us");

        when(readOnlyTransaction.executeQuery(any())).thenReturn(optionsResultSet, routinesResultSet);

        SchemaDao schemaDao = new SchemaDao(databaseClient);
        assertDoesNotThrow(() -> schemaDao.validatePlacementTvfNames("foo",
                List.of("READ_PROTO_BYTES_FOO_US")));
        verify(readOnlyTransaction, times(2)).executeQuery(any());
    }

    @Test
    void testValidatePlacementTvfNamesPostgresSuccessWithQuoting() throws SpannerException {
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.getDialect()).thenReturn(Dialect.POSTGRESQL);
        ReadOnlyTransaction readOnlyTransaction = mock(ReadOnlyTransaction.class);
        when(databaseClient.readOnlyTransaction()).thenReturn(readOnlyTransaction);

        ResultSet optionsResultSet = mock(ResultSet.class);
        when(optionsResultSet.next()).thenReturn(true, true, false);
        when(optionsResultSet.getString(0)).thenReturn("per_placement_tvf", "partition_mode");
        when(optionsResultSet.getString(1)).thenReturn("true", "MUTABLE_KEY_RANGE");

        ResultSet routinesResultSet = mock(ResultSet.class);
        when(routinesResultSet.next()).thenReturn(true, false);
        when(routinesResultSet.getString(0)).thenReturn("Read_Proto_Bytes_Foo_US");

        when(readOnlyTransaction.executeQuery(any())).thenReturn(optionsResultSet, routinesResultSet);

        SchemaDao schemaDao = new SchemaDao(databaseClient);
        assertDoesNotThrow(() -> schemaDao.validatePlacementTvfNames("foo",
                List.of("\"spanner\".\"Read_Proto_Bytes_Foo_US\"")));
    }

    @Test
    void testValidatePlacementTvfNamesNotAssociatedWithChangeStream() throws SpannerException {
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
        ReadOnlyTransaction readOnlyTransaction = mock(ReadOnlyTransaction.class);
        when(databaseClient.readOnlyTransaction()).thenReturn(readOnlyTransaction);

        ResultSet optionsResultSet = mock(ResultSet.class);
        when(optionsResultSet.next()).thenReturn(true, false);
        when(optionsResultSet.getString(0)).thenReturn("per_placement_tvf");
        when(optionsResultSet.getString(1)).thenReturn("true");

        // The TVF exists as a routine, but it belongs to a different change stream ("Other"),
        // not the one ("Foo") it was configured for.
        ResultSet routinesResultSet = mock(ResultSet.class);
        when(routinesResultSet.next()).thenReturn(true, false);
        when(routinesResultSet.getString(0)).thenReturn("READ_Other_US");

        when(readOnlyTransaction.executeQuery(any())).thenReturn(optionsResultSet, routinesResultSet);

        SchemaDao schemaDao = new SchemaDao(databaseClient);
        assertThrows(IllegalArgumentException.class,
                () -> schemaDao.validatePlacementTvfNames("Foo", List.of("READ_Other_US")));
    }

    @Test
    void testValidatePlacementTvfNamesTvfDoesNotExist() throws SpannerException {
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
        ReadOnlyTransaction readOnlyTransaction = mock(ReadOnlyTransaction.class);
        when(databaseClient.readOnlyTransaction()).thenReturn(readOnlyTransaction);

        ResultSet optionsResultSet = mock(ResultSet.class);
        when(optionsResultSet.next()).thenReturn(true, false);
        when(optionsResultSet.getString(0)).thenReturn("per_placement_tvf");
        when(optionsResultSet.getString(1)).thenReturn("true");

        ResultSet routinesResultSet = mock(ResultSet.class);
        when(routinesResultSet.next()).thenReturn(false);

        when(readOnlyTransaction.executeQuery(any())).thenReturn(optionsResultSet, routinesResultSet);

        SchemaDao schemaDao = new SchemaDao(databaseClient);
        assertThrows(IllegalArgumentException.class,
                () -> schemaDao.validatePlacementTvfNames("Foo", List.of("READ_Foo_US")));
    }

    @Test
    void testValidatePlacementTvfNamesOptionNotEnabled() throws SpannerException {
        DatabaseClient databaseClient = mock(DatabaseClient.class);
        when(databaseClient.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
        ReadOnlyTransaction readOnlyTransaction = mock(ReadOnlyTransaction.class);
        when(databaseClient.readOnlyTransaction()).thenReturn(readOnlyTransaction);

        ResultSet optionsResultSet = mock(ResultSet.class);
        when(optionsResultSet.next()).thenReturn(false);

        when(readOnlyTransaction.executeQuery(any())).thenReturn(optionsResultSet);

        SchemaDao schemaDao = new SchemaDao(databaseClient);
        assertThrows(IllegalArgumentException.class,
                () -> schemaDao.validatePlacementTvfNames("Foo", List.of("READ_Foo_US")));
    }
}