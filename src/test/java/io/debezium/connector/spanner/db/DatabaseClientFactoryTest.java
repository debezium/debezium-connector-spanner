/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import org.junit.jupiter.api.Test;

class DatabaseClientFactoryTest {

    @Test
    void testConstructor() {
        SpannerConnectorConfig spannerConnectorConfig = mock(SpannerConnectorConfig.class);
        when(spannerConnectorConfig.databaseId()).thenReturn("42");
        when(spannerConnectorConfig.gcpSpannerCredentialsJson()).thenReturn("Gcp Spanner Credentials Json");
        when(spannerConnectorConfig.gcpSpannerCredentialsPath()).thenReturn("Gcp Spanner Credentials Path");
        when(spannerConnectorConfig.instanceId()).thenReturn("42");
        when(spannerConnectorConfig.projectId()).thenReturn("myproject");
        new DatabaseClientFactory(spannerConnectorConfig);
        verify(spannerConnectorConfig).databaseId();
        verify(spannerConnectorConfig).gcpSpannerCredentialsJson();
        verify(spannerConnectorConfig).gcpSpannerCredentialsPath();
        verify(spannerConnectorConfig).instanceId();
        verify(spannerConnectorConfig).projectId();
    }

    @Test
    void testGetGoogleCredentials() {
        assertNull(new DatabaseClientFactory("myproject", "42", "42", "Credentials Json",
            "Credentials Path", null, "test-role")
            .getGoogleCredentials("Credentials Json", "Credentials Path"));
        assertNull(new DatabaseClientFactory("myproject", "42", "42", "Credentials Json",
            "Credentials Path", null, "test-role")
            .getGoogleCredentials(null, null));
        assertNull(new DatabaseClientFactory("myproject", "42", "42", "Credentials Json",
            "Credentials Path", null, "test-role")
            .getGoogleCredentials(null, "Credentials Path"));

    }

    @Test
    void testGetDatabaseClient() {
        DatabaseClientFactory databaseClientFactory = new DatabaseClientFactory("myproject", "42",
            "42", "Credentials Json", "Credentials Path", null, "test-role");

        DatabaseClient actualDatabaseClient = databaseClientFactory.getDatabaseClient();
        assertNotNull(actualDatabaseClient);
    }
}
