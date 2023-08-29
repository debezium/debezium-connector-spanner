/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.google.cloud.spanner.Dialect;

import io.debezium.connector.spanner.db.stream.SpannerChangeStream;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

class SpannerChangeStreamFactoryTest {

    @Test
    void testGetStream() {
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);
        DaoFactory daoFactory = new DaoFactory(databaseClientFactory);
        SpannerChangeStreamFactory spannerChangeStreamFactory = new SpannerChangeStreamFactory(
                "taskUid", daoFactory, new MetricsEventPublisher(), "test-connector",
                Dialect.GOOGLE_STANDARD_SQL, databaseClientFactory);
        SpannerChangeStream stream = spannerChangeStreamFactory.getStream("stream1",
                Duration.ofMillis(100), 1);
        assertNotNull(stream);
    }
}
