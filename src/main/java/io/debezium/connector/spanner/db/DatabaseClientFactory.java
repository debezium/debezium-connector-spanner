/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.spanner.Module;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.util.Strings;
import io.grpc.ManagedChannelBuilder;

/**
 * Factory for {@code DatabaseClient}
 */
public class DatabaseClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseClientFactory.class);
    private static final String USER_AGENT_PREFIX = "kafka-change-streams-connector-";

    private final String projectId;
    private final String instanceId;
    private final String databaseId;

    public static final String SPANNER_OMNI_DEFAULT_ID = "default";
    private final SpannerOptions options;
    private volatile Spanner spanner;

    private DatabaseClient databaseClient;

    public DatabaseClientFactory(String projectId, String instanceId, String databaseId,
                                 String credentialsJson,
                                 String credentialsPath, String host, String emulatorHost, String databaseRole) {

        this(projectId, instanceId, databaseId, credentialsJson, credentialsPath, host, emulatorHost, databaseRole, null, false, null, null);
    }

    public DatabaseClientFactory(String projectId, String instanceId, String databaseId,
                                 String credentialsJson,
                                 String credentialsPath, String host, String emulatorHost, String databaseRole, String spannerOmniEndpoint, boolean usePlainText,
                                 String clientKeyPath, String clientCertPath) {

        if (Strings.isNullOrEmpty(spannerOmniEndpoint)) {
            this.projectId = projectId;
            this.instanceId = instanceId;
        }
        else {
            this.projectId = SPANNER_OMNI_DEFAULT_ID;
            this.instanceId = SPANNER_OMNI_DEFAULT_ID;
        }
        this.databaseId = databaseId;

        SpannerOptions.Builder builder = SpannerOptions.newBuilder();

        GoogleCredentials googleCredentials = getGoogleCredentials(credentialsJson,
                credentialsPath);
        builder.setProjectId(this.projectId);
        if (!Strings.isNullOrEmpty(host)) {
            builder.setHost(host);
        }
        if (!Strings.isNullOrEmpty(spannerOmniEndpoint)) {
            builder.setExperimentalHost(spannerOmniEndpoint);
            builder.setCredentials(NoCredentials.getInstance());
            builder.setBuiltInMetricsEnabled(false);
            if (usePlainText) {
                builder.setChannelConfigurator(ManagedChannelBuilder::usePlaintext);
            }
            else if (!Strings.isNullOrEmpty(clientCertPath) && !Strings.isNullOrEmpty(clientKeyPath)) {
                builder.useClientCert(clientCertPath, clientKeyPath);
            }
        }
        else if (!Strings.isNullOrEmpty(emulatorHost)) {
            builder.setEmulatorHost(emulatorHost);
            builder.setCredentials(NoCredentials.getInstance());
        }
        else {
            if (googleCredentials != null) {
                builder.setCredentials(googleCredentials);
            }
        }

        if (!Strings.isNullOrEmpty(databaseRole) && Strings.isNullOrEmpty(spannerOmniEndpoint)) {
            builder.setDatabaseRole(databaseRole);
        }
        String userAgentString = USER_AGENT_PREFIX + Module.version();
        builder.setHeaderProvider(FixedHeaderProvider.create("user-agent", userAgentString));
        this.options = builder.build();
        this.spanner = options.getService();

    }

    public DatabaseClientFactory(SpannerConnectorConfig config) {
        this(config.projectId(), config.instanceId(), config.databaseId(),
                config.gcpSpannerCredentialsJson(), config.gcpSpannerCredentialsPath(),
                config.spannerHost(), config.spannerEmulatorHost(), config.databaseRole(), config.spannerOmniEndpoint(), config.usePlainText(), config.clientKeyPath(),
                config.clientCertPath());
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
        else {
            try {
                credential = ServiceAccountCredentials.getApplicationDefault();
            }
            catch (IOException e) {
                LOGGER.error("The Application Default Credentials are not available.");
                LOGGER.error(e.getMessage(), e);
            }
        }
        return credential;
    }

    public void closeSpanner() {
        synchronized (this) {
            if (spanner == null) {
                return;
            }
            try {
                spanner.close();
            }
            catch (Exception e) {
                LOGGER.error("Exception during spanner.close()", e);

            }
            spanner = null;
        }
    }

    public DatabaseClient getDatabaseClient() {
        synchronized (this) {
            if (spanner == null) {
                return null;
            }
            if (databaseClient == null) {
                databaseClient = spanner.getDatabaseClient(
                        DatabaseId.of(this.projectId, this.instanceId, this.databaseId));
            }
        }
        return databaseClient;
    }
}
