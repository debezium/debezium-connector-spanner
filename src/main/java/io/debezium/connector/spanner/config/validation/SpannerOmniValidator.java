/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.INSTANCE_ID;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.PROJECT_ID;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SPANNER_OMNI_CLIENT_CERT_PATH;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SPANNER_OMNI_CLIENT_KEY_PATH;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SPANNER_OMNI_ENDPOINT;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SPANNER_OMNI_USE_PLAINTEXT;
import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;

/**
 * Validates Spanner Omni configuration properties to ensure they follow the correct usage patterns.
 */
public class SpannerOmniValidator implements ConfigurationValidator.Validator {
    private static final Logger LOGGER = getLogger(SpannerOmniValidator.class);

    private static final String OMNI_ENDPOINT_REQUIRED_MSG = "Configuration properties for Spanner Omni can only be provided when spanner.omni.endpoint is specified";
    private static final String PLAINTEXT_AND_CERTS_CONFLICT_MSG = "When spanner.omni.use.plaintext is true, client key path and client certificate path must not be provided";
    private static final String CLIENT_CERT_AND_KEY_MUST_MATCH_MSG = "Client key path and client certificate path must be provided together";

    private final ConfigurationValidator.ValidationContext context;
    private boolean result = true;

    private SpannerOmniValidator(ConfigurationValidator.ValidationContext context) {
        this.context = context;
    }

    public static SpannerOmniValidator withContext(ConfigurationValidator.ValidationContext validationContext) {
        return new SpannerOmniValidator(validationContext);
    }

    @Override
    public SpannerOmniValidator validate() {
        String endpoint = context.getString(SPANNER_OMNI_ENDPOINT);
        String plaintext = context.getString(SPANNER_OMNI_USE_PLAINTEXT);
        String clientKeyPath = context.getString(SPANNER_OMNI_CLIENT_KEY_PATH);
        String clientCertPath = context.getString(SPANNER_OMNI_CLIENT_CERT_PATH);

        boolean endpointSpecified = FieldValidator.isSpecified(endpoint);
        boolean plaintextSpecified = FieldValidator.isSpecified(plaintext);
        boolean clientKeySpecified = FieldValidator.isSpecified(clientKeyPath);
        boolean clientCertSpecified = FieldValidator.isSpecified(clientCertPath);

        // When Spanner Omni endpoint is specified, project and instance are optional
        // (they default to "default" internally)
        if (endpointSpecified) {
            LOGGER.info("Spanner Omni endpoint specified. Clearing validation errors for project and instance fields.");
            context.clearErrors(PROJECT_ID);
            context.clearErrors(INSTANCE_ID);
        }

        // Validation 1: plaintext, client key, client cert can only be provided when endpoint is provided
        if (!endpointSpecified && (plaintextSpecified || clientKeySpecified || clientCertSpecified)) {
            LOGGER.error(OMNI_ENDPOINT_REQUIRED_MSG);
            context.error(OMNI_ENDPOINT_REQUIRED_MSG, SPANNER_OMNI_ENDPOINT);
            result = false;
            return this;
        }

        // If endpoint is not specified, we're done validating
        if (!endpointSpecified) {
            return this;
        }

        // Validation 2: When plaintext is true, client key and cert must not be provided
        if (plaintextSpecified && Boolean.parseBoolean(plaintext) && (clientKeySpecified || clientCertSpecified)) {
            LOGGER.error(PLAINTEXT_AND_CERTS_CONFLICT_MSG);
            context.error(PLAINTEXT_AND_CERTS_CONFLICT_MSG, SPANNER_OMNI_CLIENT_KEY_PATH, SPANNER_OMNI_CLIENT_CERT_PATH);
            result = false;
            return this;
        }

        // Validation 3: Client key and cert must always be provided together
        if (clientKeySpecified != clientCertSpecified) {
            LOGGER.error(CLIENT_CERT_AND_KEY_MUST_MATCH_MSG);
            context.error(CLIENT_CERT_AND_KEY_MUST_MATCH_MSG, SPANNER_OMNI_CLIENT_KEY_PATH, SPANNER_OMNI_CLIENT_CERT_PATH);
            result = false;
        }

        return this;
    }

    @Override
    public boolean isSuccess() {
        return result;
    }
}
