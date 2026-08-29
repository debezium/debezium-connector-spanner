/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

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

import io.debezium.config.Configuration;

class ConfigurationValidatorTest {

    private static Map<String, String> validConfig(String startTime, String endTime) {
        Map<String, String> config = new HashMap<>();
        config.put("gcp.spanner.project.id", "boxwood-weaver-353315");
        config.put("gcp.spanner.instance.id", "kafka-connector");
        config.put("gcp.spanner.database.id", "kafkaspan");
        config.put("gcp.spanner.database.role", "test-role");
        config.put("gcp.spanner.change.stream", "TestStream");
        config.put("gcp.spanner.start.time", startTime);
        config.put("gcp.spanner.end.time", endTime);
        config.put("heartbeat.interval.ms", "300000");
        config.put("gcp.spanner.credentials.path", "no_path");
        config.put("heartbeat.topics.prefix", "heartbeat");
        return config;
    }

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
                                "The 'gcp.spanner.database.id' value is invalid: The field is not specified")),
                Arguments.of(
                        withTasks(validConfig(startTime, endTime), "10", "2"),
                        List.of(
                                "The 'gcp.spanner.credentials.path' value is invalid: path field is incorrect",
                                "tasks.min must be less than or equal to tasks.max",
                                "tasks.min must be less than or equal to tasks.max")),
                Arguments.of(
                        withTasks(validConfig(startTime, endTime), "0", "10"),
                        List.of(
                                "The 'gcp.spanner.credentials.path' value is invalid: path field is incorrect",
                                "The 'tasks.min' value is invalid: A positive, non-zero integer value is expected")),
                Arguments.of(
                        withTasks(validConfig(startTime, endTime), "-1", "10"),
                        List.of(
                                "The 'gcp.spanner.credentials.path' value is invalid: path field is incorrect",
                                "The 'tasks.min' value is invalid: A positive, non-zero integer value is expected")),
                Arguments.of(
                        withTasks(validConfig(startTime, endTime), "2", "0"),
                        List.of(
                                "The 'gcp.spanner.credentials.path' value is invalid: path field is incorrect",
                                "The 'tasks.max' value is invalid: A positive, non-zero integer value is expected")),
                Arguments.of(
                        withTasks(validConfig(startTime, endTime), "2", "-1"),
                        List.of(
                                "The 'gcp.spanner.credentials.path' value is invalid: path field is incorrect",
                                "The 'tasks.max' value is invalid: A positive, non-zero integer value is expected")));
    }

    private static Map<String, String> withTasks(Map<String, String> config, String tasksMin, String tasksMax) {
        config.put("tasks.min", tasksMin);
        config.put("tasks.max", tasksMax);
        return config;
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
