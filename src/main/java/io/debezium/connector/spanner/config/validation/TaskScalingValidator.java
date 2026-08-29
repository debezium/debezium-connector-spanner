/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.MAX_TASKS;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.MIN_TASKS;
import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;

/**
 * Validate that the minimum number of tasks does not exceed the maximum number of tasks
 */
public class TaskScalingValidator implements ConfigurationValidator.Validator {

    private static final Logger LOGGER = getLogger(TaskScalingValidator.class);
    private final ConfigurationValidator.ValidationContext validationContext;
    private boolean result = true;

    public TaskScalingValidator(ConfigurationValidator.ValidationContext validationContext) {
        this.validationContext = validationContext;
    }

    public static TaskScalingValidator withContext(ConfigurationValidator.ValidationContext validationContext) {
        return new TaskScalingValidator(validationContext);
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

        int minTasks = Integer.parseInt(validationContext.getString(MIN_TASKS));
        int maxTasks = Integer.parseInt(validationContext.getString(MAX_TASKS));

        if (minTasks > maxTasks) {
            String msg = "tasks.min must be less than or equal to tasks.max";
            LOGGER.error(msg);
            validationContext.error(msg, MIN_TASKS, MAX_TASKS);
            result = false;
        }

        return this;
    }

    private boolean canValidate() {
        return validationContext.getErrors(MAX_TASKS).isEmpty() && validationContext.getErrors(MIN_TASKS).isEmpty();
    }

}
