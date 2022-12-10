/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.spanner.SpannerConnectorConfig;

/**
 * Validates all the properties of the Connector configuration
 */
public class ConfigurationValidator {

    private ConfigurationValidator() {
    }

    /**
     * Validates configuration properties
     * @param connectorConfigs map of configuration properties
     * @return Config object
     */
    public static Config validate(Map<String, String> connectorConfigs) {
        Configuration config = Configuration.from(connectorConfigs);

        // perform filed validation withValidation specified validators
        Map<String, ConfigValue> results = config.validate(SpannerConnectorConfig.ALL_FIELDS);

        ValidationContext validationContext = new ValidationContext(config, results);

        // high level validation
        ConnectionValidator.withContext(validationContext).validate()
                .then(ChangeStreamValidator.withContext(validationContext));
        StartEndTimeValidator.withContext(validationContext).validate();

        return new Config(validationContext.getResults());
    }

    /**
     * Context to store validation results and config
     */
    public static class ValidationContext {
        private final Configuration config;
        private final Map<String, ConfigValue> configValueMap;

        public ValidationContext(Configuration config, Map<String, ConfigValue> configValueMap) {
            this.config = config;
            this.configValueMap = configValueMap;
        }

        /**
         * Gets configuration string
         * @param field field object
         * @return value of configuration property
         */
        public String getString(Field field) {
            return this.config.getString(field);
        }

        /**
         * Gets errors string list for configuration field
         * @param field field object
         * @return errors string list
         */
        public List<String> getErrors(Field field) {
            return this.configValueMap.get(field.name()).errorMessages();
        }

        /**
         * Register errors for fields
         * @param fields array of field object
         */
        public void error(String message, Field... fields) {
            if (fields.length == 0) {
                throw new IllegalArgumentException("fields must be specified");
            }
            for (Field field : fields) {
                this.configValueMap.get(field.name()).addErrorMessage(message);
            }
        }

        /**
         * Gets results of validation
         * @return
         */
        public List<ConfigValue> getResults() {
            return new ArrayList<>(configValueMap.values());
        }

    }

    public interface Validator {

        Validator validate();

        boolean isSuccess();

        default Validator then(Validator validator) {
            if (isSuccess()) {
                return validator.validate();
            }
            return this;
        }

    }

}
