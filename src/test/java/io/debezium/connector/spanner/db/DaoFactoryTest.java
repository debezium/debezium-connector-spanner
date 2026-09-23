/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.spanner.Options;

import io.debezium.connector.spanner.db.dao.ChangeStreamDao;
import io.debezium.connector.spanner.db.dao.SchemaDao;
import io.debezium.connector.spanner.db.model.ChangeStreamOptions;

class DaoFactoryTest {

    @Test
    void testGetSchemaDao() {
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);
        DaoFactory daoFactory = new DaoFactory(databaseClientFactory);
        SchemaDao schemaDao = daoFactory.getSchemaDao();
        assertNotNull(schemaDao);
    }

    @Test
    void testGetStreamDao() {
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);
        SchemaDao mockSchema = mock(SchemaDao.class);
        when(mockSchema.getChangeStreamOptions("")).thenReturn(new ChangeStreamOptions(true, false));

        // use a spy so we can stub getSchemaDao() to return our mockSchema
        DaoFactory daoFactory = spy(new DaoFactory(databaseClientFactory));
        doReturn(mockSchema).when(daoFactory).getSchemaDao();

        String changeStreamName = "";
        Options.RpcPriority rpcPriority = Options.RpcPriority.LOW;
        String jobName = "";

        ChangeStreamDao actualStreamDao = daoFactory.getStreamDao(changeStreamName, rpcPriority, jobName);
        assertNotNull(actualStreamDao);
        verify(mockSchema, times(1)).getChangeStreamOptions(changeStreamName);
    }

    @Test
    void testGetStreamDaoWithPlacementTvfNamesValidatesAgainstSchema() {
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);
        SchemaDao mockSchema = mock(SchemaDao.class);
        ChangeStreamOptions streamOptions = new ChangeStreamOptions(true, true);
        when(mockSchema.getChangeStreamOptions("Foo")).thenReturn(streamOptions);

        DaoFactory daoFactory = spy(new DaoFactory(databaseClientFactory));
        doReturn(mockSchema).when(daoFactory).getSchemaDao();

        String changeStreamName = "Foo";
        List<String> placementTvfNames = List.of("READ_Foo_US", "READ_Foo_EU");
        Options.RpcPriority rpcPriority = Options.RpcPriority.LOW;
        String jobName = "";

        ChangeStreamDao actualStreamDao = daoFactory.getStreamDao(changeStreamName, placementTvfNames, rpcPriority, jobName);

        assertNotNull(actualStreamDao);
        verify(mockSchema, times(1)).validatePlacementTvfNames(changeStreamName, placementTvfNames, streamOptions);
    }

    @Test
    void testGetStreamDaoWithPlacementTvfNamesFailsWhenNotMutableKeyRange() {
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);
        SchemaDao mockSchema = mock(SchemaDao.class);
        when(mockSchema.getChangeStreamOptions("Foo")).thenReturn(new ChangeStreamOptions(false, true));

        DaoFactory daoFactory = spy(new DaoFactory(databaseClientFactory));
        doReturn(mockSchema).when(daoFactory).getSchemaDao();

        String changeStreamName = "Foo";
        List<String> placementTvfNames = List.of("READ_Foo_US");

        assertThrows(IllegalArgumentException.class,
                () -> daoFactory.getStreamDao(changeStreamName, placementTvfNames, Options.RpcPriority.LOW, ""));
    }
}
