/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

import com.google.auth.oauth2.GoogleCredentials;

import io.debezium.config.Configuration;

class ConnectionValidatorTest {

    private static Stream<Arguments> configProvider() {
        return Stream.of(
                Arguments.of(
                        Configuration.from(Map.of(
                                "gcp.spanner.project.id", "boxwood-weaver-353315",
                                "gcp.spanner.instance.id", "kafka-connector",
                                "gcp.spanner.database.id", "kafkaspan",
                                "gcp.spanner.credentials.path", "/path/to/credential"))),
                Arguments.of(
                        Configuration.from(Map.of(
                                "gcp.spanner.project.id", "boxwood-weaver-353315",
                                "gcp.spanner.instance.id", "kafka-connector",
                                "gcp.spanner.database.id", "kafkaspan",
                                "gcp.spanner.credentials.json", "{}"))),
                Arguments.of(
                        Configuration.from(Map.of(
                                "gcp.spanner.project.id", "boxwood-weaver-353315",
                                "gcp.spanner.instance.id", "kafka-connector",
                                "gcp.spanner.database.id", "kafkaspan"))));
    }

    @ParameterizedTest
    @MethodSource("configProvider")
    void validateSuccess(Configuration configuration) {
        try (MockedStatic<GoogleCredentials> credentials = mockStatic(GoogleCredentials.class)) {
            credentials.when(GoogleCredentials::getApplicationDefault).thenReturn(null);

            Map<String, ConfigValue> configValueMap = Map.of(
                    "gcp.spanner.project.id", new ConfigValue("gcp.spanner.project.id"),
                    "gcp.spanner.instance.id", new ConfigValue("gcp.spanner.instance.id"),
                    "gcp.spanner.database.id", new ConfigValue("gcp.spanner.database.id"),
                    "gcp.spanner.credentials.json", new ConfigValue("gcp.spanner.credentials.json"),
                    "gcp.spanner.credentials.path", new ConfigValue("gcp.spanner.credentials.path"));

            ConfigurationValidator.ValidationContext validationContext = new ConfigurationValidator.ValidationContext(configuration, configValueMap);
            ConnectionValidator connectionValidator = spy(ConnectionValidator.withContext(validationContext));

            Assertions.assertTrue(connectionValidator.isSuccess());
            connectionValidator.validate();
            Assertions.assertTrue(connectionValidator.isSuccess());
        }
    }

    @Test
    void validateNotSuccess() {
        Configuration configuration = Configuration.from(Map.of(
                "gcp.spanner.project.id", "boxwood-weaver-353315",
                "gcp.spanner.instance.id", "kafka-connector",
                "gcp.spanner.database.id", "kafkaspan"));

        try (MockedStatic<GoogleCredentials> credentials = mockStatic(GoogleCredentials.class)) {
            credentials.when(GoogleCredentials::getApplicationDefault).thenThrow(new IOException());

            Map<String, ConfigValue> configValueMap = Map.of(
                    "gcp.spanner.project.id", new ConfigValue("gcp.spanner.project.id"),
                    "gcp.spanner.instance.id", new ConfigValue("gcp.spanner.instance.id"),
                    "gcp.spanner.database.id", new ConfigValue("gcp.spanner.database.id"),
                    "gcp.spanner.credentials.json", new ConfigValue("gcp.spanner.credentials.json"),
                    "gcp.spanner.credentials.path", new ConfigValue("gcp.spanner.credentials.path"));

            ConfigurationValidator.ValidationContext validationContext = new ConfigurationValidator.ValidationContext(configuration, configValueMap);
            ConnectionValidator connectionValidator = spy(ConnectionValidator.withContext(validationContext));

            Assertions.assertTrue(connectionValidator.isSuccess());
            connectionValidator.validate();
            Assertions.assertFalse(connectionValidator.isSuccess());
        }
    }
}
