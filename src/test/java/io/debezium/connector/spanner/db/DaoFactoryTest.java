/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import com.google.cloud.spanner.Options;

import io.debezium.connector.spanner.db.dao.ChangeStreamDao;
import io.debezium.connector.spanner.db.dao.SchemaDao;

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
        when(mockSchema.isMutableKeyRangeChangeStream(any())).thenReturn(true);

        // use a spy so we can stub getSchemaDao() to return our mockSchema
        DaoFactory daoFactory = spy(new DaoFactory(databaseClientFactory));
        doReturn(mockSchema).when(daoFactory).getSchemaDao();

        String changeStreamName = "";
        Options.RpcPriority rpcPriority = Options.RpcPriority.LOW;
        String jobName = "";

        ChangeStreamDao actualStreamDao = daoFactory.getStreamDao(changeStreamName, rpcPriority, jobName);
        assertNotNull(actualStreamDao);
        verify(mockSchema, times(1)).isMutableKeyRangeChangeStream(changeStreamName);
    }
}
