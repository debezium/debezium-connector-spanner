/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.INSTANCE_ID;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.PROJECT_ID;

/**
 * Validates Cloud Spanner configuration properties.
 */
public class CloudSpannerValidator implements ConfigurationValidator.Validator {
    private static final String FIELD_NOT_SPECIFIED_MSG = "The '%s' value is invalid: The field is not specified";

    private final ConfigurationValidator.ValidationContext context;
    private boolean result = true;

    private CloudSpannerValidator(ConfigurationValidator.ValidationContext context) {
        this.context = context;
    }

    public static CloudSpannerValidator withContext(ConfigurationValidator.ValidationContext validationContext) {
        return new CloudSpannerValidator(validationContext);
    }

    @Override
    public CloudSpannerValidator validate() {
        String projectId = context.getString(PROJECT_ID);
        String instanceId = context.getString(INSTANCE_ID);

        if (!FieldValidator.isSpecified(projectId)) {
            context.error(String.format(FIELD_NOT_SPECIFIED_MSG, PROJECT_ID.name()), PROJECT_ID);
            result = false;
        }

        if (!FieldValidator.isSpecified(instanceId)) {
            context.error(String.format(FIELD_NOT_SPECIFIED_MSG, INSTANCE_ID.name()), INSTANCE_ID);
            result = false;
        }

        return this;
    }

    @Override
    public boolean isSuccess() {
        return result;
    }
}
