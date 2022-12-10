/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.END_TIME;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.START_TIME;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Instant;

import org.slf4j.Logger;

/**
 * Validate if start and end timestamps of connector match each other
 */
public class StartEndTimeValidator implements ConfigurationValidator.Validator {

    private static final Logger LOGGER = getLogger(StartEndTimeValidator.class);
    private final ConfigurationValidator.ValidationContext validationContext;
    private boolean result = true;

    public StartEndTimeValidator(ConfigurationValidator.ValidationContext validationContext) {
        this.validationContext = validationContext;
    }

    public static StartEndTimeValidator withContext(ConfigurationValidator.ValidationContext validationContext) {
        return new StartEndTimeValidator(validationContext);
    }

    @Override
    public boolean isSuccess() {
        return result;
    }

    @Override
    public ConfigurationValidator.Validator validate() {
        if (!canValidate()) {
            result = false;
            return this;
        }

        String startTime = validationContext.getString(START_TIME);
        String endTime = validationContext.getString(END_TIME);

        if (endTime != null && Instant.parse(startTime).isAfter(Instant.parse(endTime))) {
            String msg = "End time must be after start time";
            LOGGER.error(msg);
            validationContext.error(msg, END_TIME);
            result = false;
        }

        return this;
    }

    private boolean canValidate() {
        return validationContext.getErrors(START_TIME).isEmpty() && validationContext.getErrors(END_TIME).isEmpty();
    }

}
