/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import io.debezium.config.Configuration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConfigurationValidatorTest {

    private static Stream<Arguments> configProvider() {
        String startTime = Instant.ofEpochMilli(Instant.now().toEpochMilli() - 10000).toString();

        String endTime = Instant.ofEpochMilli(Instant.now().toEpochMilli() + 10000).toString();

        return Stream.of(
                Arguments.of(
                        Map.of(
                            "gcp.spanner.project.id", "boxwood-weaver-353315",
                            "gcp.spanner.instance.id", "kafka-connector",
                            "gcp.spanner.database.id", "kafkaspan",
                            "gcp.spanner.database.role", "test-role",
                            "gcp.spanner.change.stream", "TestStream",
                            "gcp.spanner.start.time", startTime,
                            "gcp.spanner.end.time", endTime,
                            "heartbeat.interval.ms", "300000",
                            "gcp.spanner.credentials.path", "no_path",
                            "heartbeat.topics.prefix", "heartbeat"),
                        List.of("The 'gcp.spanner.credentials.path' value is invalid: path field is incorrect")),
                Arguments.of(
                        Map.of(
                            "gcp.spanner.instance.id", "kafka-connector",
                            "gcp.spanner.database.id", "kafkaspan",
                            "gcp.spanner.change.stream", "TestStream",
                            "gcp.spanner.database.role", "test-role",
                            "gcp.spanner.start.time", startTime,
                            "gcp.spanner.end.time", endTime,
                            "heartbeat.interval.ms", "300000",
                            "heartbeat.topics.prefix", "heartbeat"),
                        List.of("The 'gcp.spanner.project.id' value is invalid: The field is not specified")),
                Arguments.of(
                        Map.of(), List.of(
                                "The 'gcp.spanner.instance.id' value is invalid: The field is not specified",
                                "The 'gcp.spanner.project.id' value is invalid: The field is not specified",
                                "The 'gcp.spanner.change.stream' value is invalid: The field is not specified",
                                "The 'gcp.spanner.database.id' value is invalid: The field is not specified")));
    }

    @ParameterizedTest
    @MethodSource("configProvider")
    void validate(Map<String, String> connectorConfigs, List<String> expectedErrors) {
        Config config = ConfigurationValidator.validate(connectorConfigs);

        List<String> errors = config.configValues()
                .stream()
                .map(ConfigValue::errorMessages)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        Assertions.assertLinesMatch(expectedErrors, errors);
    }

    @Test
    void testValidationContextError() {
        Configuration config = mock(Configuration.class);
        assertThrows(IllegalArgumentException.class,
                () -> (new ConfigurationValidator.ValidationContext(config, new HashMap<>()))
                        .error("error"));
    }
}
