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
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.db.dao.SchemaDao;
import io.debezium.connector.spanner.db.stream.SpannerChangeStream;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

class SpannerChangeStreamFactoryTest {

    @Test
    void testGetStream() {
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);
        SchemaDao mockSchema = mock(SchemaDao.class);
        when(mockSchema.isMutableKeyRangeChangeStream(any())).thenReturn(true);

        // use a spy so we can stub getSchemaDao() to return our mockSchema
        DaoFactory daoFactory = spy(new DaoFactory(databaseClientFactory));
        doReturn(mockSchema).when(daoFactory).getSchemaDao();

        SpannerChangeStreamFactory spannerChangeStreamFactory = new SpannerChangeStreamFactory(
                "taskUid", daoFactory, new MetricsEventPublisher(), "test-connector",
                databaseClientFactory);
        SpannerChangeStream stream = spannerChangeStreamFactory.getStream("stream1",
                Duration.ofMillis(100), 1);
        assertNotNull(stream);
    }
}
