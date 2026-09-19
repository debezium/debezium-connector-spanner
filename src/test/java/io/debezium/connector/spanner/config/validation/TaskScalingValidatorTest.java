/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.config.Configuration;

class TaskScalingValidatorTest {

    private static Stream<Arguments> configProvider() {

        return Stream.of(
                Arguments.of(
                        Configuration.from(Map.of(
                                "tasks.min", "2",
                                "tasks.max", "10")),
                        true),
                Arguments.of(
                        Configuration.from(Map.of(
                                "tasks.min", "5",
                                "tasks.max", "5")),
                        true),
                Arguments.of(
                        Configuration.from(Map.of()),
                        true),
                Arguments.of(
                        Configuration.from(Map.of(
                                "tasks.min", "10",
                                "tasks.max", "2")),
                        false));
    }

    @ParameterizedTest
    @MethodSource("configProvider")
    void validate(Configuration configuration, boolean isSuccess) {
        Map<String, ConfigValue> configValueMap = Map.of(
                "tasks.min", new ConfigValue("tasks.min"),
                "tasks.max", new ConfigValue("tasks.max"));

        ConfigurationValidator.ValidationContext validationContext = new ConfigurationValidator.ValidationContext(configuration, configValueMap);

        TaskScalingValidator taskScalingValidator = TaskScalingValidator.withContext(validationContext);
        taskScalingValidator.validate();
        Assertions.assertEquals(isSuccess, taskScalingValidator.isSuccess());
    }
}
