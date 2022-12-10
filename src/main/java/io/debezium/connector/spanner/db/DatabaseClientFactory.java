/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.spanner.SpannerConnectorConfig;

/**
 * Factory for {@code DatabaseClient}
 */
public class DatabaseClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseClientFactory.class);

    private final String projectId;
    private final String instanceId;
    private final String databaseId;

    private final SpannerOptions options;

    private DatabaseClient databaseClient;

    public DatabaseClientFactory(String projectId, String instanceId, String databaseId, String credentialsJson,
                                 String credentialsPath, String host) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.databaseId = databaseId;

        SpannerOptions.Builder builder = SpannerOptions.newBuilder();

        GoogleCredentials googleCredentials = getGoogleCredentials(credentialsJson, credentialsPath);
        if (googleCredentials != null) {
            builder.setCredentials(googleCredentials);
        }
        builder.setProjectId(this.projectId);
        if (isNotEmpty(host)) {
            builder.setHost(host);
        }

        this.options = builder.build();
    }

    public DatabaseClientFactory(SpannerConnectorConfig config) {
        this(config.projectId(), config.instanceId(), config.databaseId(),
                config.gcpSpannerCredentialsJson(), config.gcpSpannerCredentialsPath(), config.spannerHost());
    }

    @VisibleForTesting
    GoogleCredentials getGoogleCredentials(String credentialsJson, String credentialsPath) {
        GoogleCredentials credential = null;
        if (credentialsJson != null) {
            try {
                credential = GoogleCredentials.fromStream(new ByteArrayInputStream(credentialsJson.getBytes()));
            }
            catch (IOException ex) {
                LOGGER.error("Error read GOOGLE CREDENTIALS from params {}", credentialsJson);
                LOGGER.error(ex.getMessage(), ex);
            }
        }
        else if (credentialsPath != null) {
            try {
                credential = GoogleCredentials.fromStream(new FileInputStream(credentialsPath));
            }
            catch (IOException e) {
                LOGGER.error("Error read GOOGLE CREDENTIALS from path {}", credentialsPath);
                LOGGER.error(e.getMessage(), e);
            }
        }
        return credential;
    }

    public DatabaseClient getDatabaseClient() {
        if (databaseClient != null) {
            return databaseClient;
        }
        databaseClient = options.getService().getDatabaseClient(
                DatabaseId.of(this.projectId, this.instanceId, this.databaseId));
        return databaseClient;
    }
}
