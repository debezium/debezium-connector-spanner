/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

class ChangeStreamValidatorTest {

    @Test
    void validateSuccess() {
        Configuration configuration = Configuration.from(Map.of(
                "gcp.spanner.project.id", "boxwood-weaver-353315",
                "gcp.spanner.instance.id", "kafka-connector",
                "gcp.spanner.database.id", "kafkaspan",
                "gcp.spanner.change.stream", "TestStream",
                "gcp.spanner.start.time", "2022-07-15T10:25:13.905049Z",
                "gcp.spanner.end.time", "2022-07-15T10:25:14.905049Z",
                "heartbeat.interval.ms", "300000",
                "heartbeat.topics.prefix", "heartbeat"));

        ConfigurationValidator.ValidationContext validationContext = new ConfigurationValidator.ValidationContext(configuration, Map.of());
        ChangeStreamValidator changeStreamValidator = spy(ChangeStreamValidator.withContext(validationContext));
        doReturn(true).when(changeStreamValidator).isStreamExist(any(), eq("TestStream"));

        Assertions.assertFalse(changeStreamValidator.isSuccess());
        changeStreamValidator.validate();
        Assertions.assertTrue(changeStreamValidator.isSuccess());
    }

    @Test
    void validateNotSuccess() {
        Configuration configuration = Configuration.from(Map.of(
                "gcp.spanner.project.id", "boxwood-weaver-353315",
                "gcp.spanner.instance.id", "kafka-connector",
                "gcp.spanner.database.id", "kafkaspan",
                "gcp.spanner.start.time", "2022-07-15T10:25:13.905049Z",
                "gcp.spanner.end.time", "2022-07-15T10:25:14.905049Z",
                "heartbeat.interval.ms", "300000",
                "heartbeat.topics.prefix", "heartbeat"));

        ConfigurationValidator.ValidationContext validationContext = new ConfigurationValidator.ValidationContext(configuration,
                Map.of("gcp.spanner.change.stream", new ConfigValue("gcp.spanner.change.stream")));
        ChangeStreamValidator changeStreamValidator = spy(ChangeStreamValidator.withContext(validationContext));
        doReturn(false).when(changeStreamValidator).isStreamExist(any(), any());

        Assertions.assertFalse(changeStreamValidator.isSuccess());
        changeStreamValidator.validate();
        Assertions.assertFalse(changeStreamValidator.isSuccess());
    }
}
