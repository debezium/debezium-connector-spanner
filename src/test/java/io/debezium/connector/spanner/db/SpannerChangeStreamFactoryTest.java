/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.db.stream.SpannerChangeStream;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

class SpannerChangeStreamFactoryTest {

    @Test
    void testGetStream() {
        DaoFactory daoFactory = new DaoFactory(
                new DatabaseClientFactory(
                        "myproject", "42", "42", "Credentials Json", "Credentials Path", null, "test-role"));
        SpannerChangeStreamFactory spannerChangeStreamFactory = new SpannerChangeStreamFactory(
                daoFactory, new MetricsEventPublisher(), "test-connector");
        SpannerChangeStream stream = spannerChangeStreamFactory.getStream("stream1",
                Duration.ofMillis(100), 1);
        assertNotNull(stream);
    }
}
