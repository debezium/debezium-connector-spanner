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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.SpannerConnectorConfig;

/**
 * Tests for SpannerOmniValidator
 */
class SpannerOmniValidatorTest {

    private Config validateConfig(Map<String, String> connectorConfigs) {
        Configuration config = Configuration.from(connectorConfigs);
        Map<String, ConfigValue> results = config.validate(SpannerConnectorConfig.ALL_FIELDS);
        ConfigurationValidator.ValidationContext validationContext = new ConfigurationValidator.ValidationContext(config, results);
        SpannerOmniValidator.withContext(validationContext).validate();
        return new Config(validationContext.getResults());
    }

    @Test
    void testValidConfigWithPlaintextAndNoClientCerts() {
        // Valid: plaintext enabled without client certificates and with endpoint
        Map<String, String> configs = new HashMap<>();
        configs.put(SPANNER_OMNI_ENDPOINT.name(), "localhost:5432");
        configs.put(SPANNER_OMNI_USE_PLAINTEXT.name(), "true");

        Config result = validateConfig(configs);
        ConfigValue endpointValue = result.configValues().stream()
                .filter(cv -> cv.name().equals(SPANNER_OMNI_ENDPOINT.name()))
                .findFirst()
                .orElse(null);
        assertTrue(endpointValue != null && endpointValue.errorMessages().isEmpty());
    }

    @Test
    void testValidConfigWithClientCertsAndNoPlaintext() {
        // Valid: client certs provided without plaintext being true
        Map<String, String> configs = new HashMap<>();
        configs.put(SPANNER_OMNI_ENDPOINT.name(), "localhost:5432");
        configs.put(SPANNER_OMNI_CLIENT_KEY_PATH.name(), "/path/to/key");
        configs.put(SPANNER_OMNI_CLIENT_CERT_PATH.name(), "/path/to/cert");

        Config result = validateConfig(configs);
        ConfigValue keyValue = result.configValues().stream()
                .filter(cv -> cv.name().equals(SPANNER_OMNI_CLIENT_KEY_PATH.name()))
                .findFirst()
                .orElse(null);
        assertTrue(keyValue != null && keyValue.errorMessages().isEmpty());
    }

    @Test
    void testInvalidConfigWithoutEndpoint() {
        // Invalid: plaintext provided without endpoint
        Map<String, String> configs = new HashMap<>();
        configs.put(SPANNER_OMNI_USE_PLAINTEXT.name(), "true");

        Config result = validateConfig(configs);
        ConfigValue endpointValue = result.configValues().stream()
                .filter(cv -> cv.name().equals(SPANNER_OMNI_ENDPOINT.name()))
                .findFirst()
                .orElse(null);
        assertTrue(endpointValue != null && !endpointValue.errorMessages().isEmpty());
    }

    @Test
    void testInvalidConfigClientCertsWithoutEndpoint() {
        // Invalid: client certs provided without endpoint
        Map<String, String> configs = new HashMap<>();
        configs.put(SPANNER_OMNI_CLIENT_KEY_PATH.name(), "/path/to/key");
        configs.put(SPANNER_OMNI_CLIENT_CERT_PATH.name(), "/path/to/cert");

        Config result = validateConfig(configs);
        ConfigValue endpointValue = result.configValues().stream()
                .filter(cv -> cv.name().equals(SPANNER_OMNI_ENDPOINT.name()))
                .findFirst()
                .orElse(null);
        assertTrue(endpointValue != null && !endpointValue.errorMessages().isEmpty());
    }

    @Test
    void testInvalidConfigPlaintextWithClientCerts() {
        // Invalid: plaintext enabled but client certs are provided
        Map<String, String> configs = new HashMap<>();
        configs.put(SPANNER_OMNI_ENDPOINT.name(), "localhost:5432");
        configs.put(SPANNER_OMNI_USE_PLAINTEXT.name(), "true");
        configs.put(SPANNER_OMNI_CLIENT_KEY_PATH.name(), "/path/to/key");
        configs.put(SPANNER_OMNI_CLIENT_CERT_PATH.name(), "/path/to/cert");

        Config result = validateConfig(configs);
        ConfigValue keyValue = result.configValues().stream()
                .filter(cv -> cv.name().equals(SPANNER_OMNI_CLIENT_KEY_PATH.name()))
                .findFirst()
                .orElse(null);
        assertTrue(keyValue != null && !keyValue.errorMessages().isEmpty());
    }

    @Test
    void testInvalidConfigMissingClientKeyPath() {
        // Invalid: client cert provided without client key
        Map<String, String> configs = new HashMap<>();
        configs.put(SPANNER_OMNI_ENDPOINT.name(), "localhost:5432");
        configs.put(SPANNER_OMNI_CLIENT_CERT_PATH.name(), "/path/to/cert");

        Config result = validateConfig(configs);
        ConfigValue keyValue = result.configValues().stream()
                .filter(cv -> cv.name().equals(SPANNER_OMNI_CLIENT_KEY_PATH.name()))
                .findFirst()
                .orElse(null);
        assertTrue(keyValue != null && !keyValue.errorMessages().isEmpty());
    }

    @Test
    void testInvalidConfigMissingClientCertPath() {
        // Invalid: client key provided without client cert
        Map<String, String> configs = new HashMap<>();
        configs.put(SPANNER_OMNI_ENDPOINT.name(), "localhost:5432");
        configs.put(SPANNER_OMNI_CLIENT_KEY_PATH.name(), "/path/to/key");

        Config result = validateConfig(configs);
        ConfigValue certValue = result.configValues().stream()
                .filter(cv -> cv.name().equals(SPANNER_OMNI_CLIENT_CERT_PATH.name()))
                .findFirst()
                .orElse(null);
        assertTrue(certValue != null && !certValue.errorMessages().isEmpty());
    }

    @Test
    void testValidConfigNoOmniSettings() {
        // Valid: no Omni settings at all (using regular GCP Spanner)
        Map<String, String> configs = new HashMap<>();

        Config result = validateConfig(configs);
        ConfigValue endpointValue = result.configValues().stream()
                .filter(cv -> cv.name().equals(SPANNER_OMNI_ENDPOINT.name()))
                .findFirst()
                .orElse(null);
        assertTrue(endpointValue != null && endpointValue.errorMessages().isEmpty());
    }

    @Test
    void testValidConfigPlaintextFalseWithClientCerts() {
        // Valid: plaintext is false and client certs are provided
        Map<String, String> configs = new HashMap<>();
        configs.put(SPANNER_OMNI_ENDPOINT.name(), "localhost:5432");
        configs.put(SPANNER_OMNI_USE_PLAINTEXT.name(), "false");
        configs.put(SPANNER_OMNI_CLIENT_KEY_PATH.name(), "/path/to/key");
        configs.put(SPANNER_OMNI_CLIENT_CERT_PATH.name(), "/path/to/cert");

        Config result = validateConfig(configs);
        ConfigValue keyValue = result.configValues().stream()
                .filter(cv -> cv.name().equals(SPANNER_OMNI_CLIENT_KEY_PATH.name()))
                .findFirst()
                .orElse(null);
        assertTrue(keyValue != null && keyValue.errorMessages().isEmpty());
    }

    @Test
    void testValidConfigOmniEndpointClearsProjectInstanceErrors() {
        // Valid: when Spanner Omni endpoint is specified, project and instance validation errors should be cleared
        Map<String, String> configs = new HashMap<>();
        configs.put(SPANNER_OMNI_ENDPOINT.name(), "localhost:5432");
        configs.put(SPANNER_OMNI_USE_PLAINTEXT.name(), "true");

        Config result = validateConfig(configs);

        // Verify that project, instance, database, and change stream have no errors despite not being provided
        ConfigValue projectValue = result.configValues().stream()
                .filter(cv -> cv.name().equals(PROJECT_ID.name()))
                .findFirst()
                .orElse(null);
        assertTrue(projectValue != null && projectValue.errorMessages().isEmpty(),
                "PROJECT_ID should have no errors when Spanner Omni endpoint is specified");

        ConfigValue instanceValue = result.configValues().stream()
                .filter(cv -> cv.name().equals(INSTANCE_ID.name()))
                .findFirst()
                .orElse(null);
        assertTrue(instanceValue != null && instanceValue.errorMessages().isEmpty(),
                "INSTANCE_ID should have no errors when Spanner Omni endpoint is specified");
    }
}
