/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.config.Configuration;

class StartEndTimeValidatorTest {

    private static Stream<Arguments> configProvider() {

        return Stream.of(
                Arguments.of(
                        Configuration.from(Map.of(
                                "gcp.spanner.start.time", Instant.ofEpochMilli(Instant.now().toEpochMilli() - 10000).toString(),
                                "gcp.spanner.end.time", Instant.ofEpochMilli(Instant.now().toEpochMilli() + 10000).toString())),
                        true),
                Arguments.of(
                        Configuration.from(Map.of()),
                        true),
                Arguments.of(
                        Configuration.from(Map.of(
                                "gcp.spanner.start.time", Instant.ofEpochMilli(Instant.now().toEpochMilli() + 100001).toString(),
                                "gcp.spanner.end.time", Instant.ofEpochMilli(Instant.now().toEpochMilli() + 10000).toString())),
                        false));
    }

    @ParameterizedTest
    @MethodSource("configProvider")
    void validate(Configuration configuration, boolean isSuccess) {
        Map<String, ConfigValue> configValueMap = Map.of(
                "gcp.spanner.start.time", new ConfigValue("gcp.spanner.start.time"),
                "gcp.spanner.end.time", new ConfigValue("gcp.spanner.end.time"));

        ConfigurationValidator.ValidationContext validationContext = new ConfigurationValidator.ValidationContext(configuration, configValueMap);

        StartEndTimeValidator startEndTimeValidator = StartEndTimeValidator.withContext(validationContext);
        startEndTimeValidator.validate();
        Assertions.assertEquals(isSuccess, startEndTimeValidator.isSuccess());
    }
}
