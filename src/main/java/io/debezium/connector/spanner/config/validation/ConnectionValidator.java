/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.DATABASE_ID;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.INSTANCE_ID;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.PROJECT_ID;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SPANNER_CREDENTIALS_JSON;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SPANNER_CREDENTIALS_PATH;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SPANNER_EMULATOR_HOST;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SPANNER_HOST;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;

import org.slf4j.Logger;

import com.google.auth.oauth2.ServiceAccountCredentials;

/**
 * Checks if the connection to database could be established by given configuration
 */
public class ConnectionValidator implements ConfigurationValidator.Validator {
    private static final Logger LOGGER = getLogger(ConnectionValidator.class);

    private static final String GOOGLE_APPLICATION_CREDENTIALS_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS";

    private static final String PLEASE_SPECIFY_CONFIGURATION_PROPERTY_MSG = "Configuration property %s or %s is not specified; Application Default Credentials will be used.";

    private static final String GOOGLE_CREDENTIAL_INCORRECT = "Can`t connect to spanner. Google credential is incorrect";
    private static final String INSTANCE_NOT_EXIST = "Instance %s does not exist";
    private static final String CONNECTOR_NOT_SUPPORT_POSTGRESQL_DIALECT = "Spanner connector doesn't support PostgreSql dialect";
    private static final String DATABASE_ID_NOT_EXIST = "Database %s does not exist";
    private static final String HOST_CONFLICT = "Can`t specify configuration properties for both production cloud spanner host and cloud spanner emulator host.";

    private final ConfigurationValidator.ValidationContext context;
    private boolean result = true;

    private ConnectionValidator(ConfigurationValidator.ValidationContext context) {
        this.context = context;
    }

    public static ConnectionValidator withContext(ConfigurationValidator.ValidationContext validationContext) {
        return new ConnectionValidator(validationContext);
    }

    @Override
    public ConnectionValidator validate() {
        String host = context.getString(SPANNER_HOST);
        String emulatorHost = context.getString(SPANNER_EMULATOR_HOST);
        boolean isAgainstEmulator = FieldValidator.isSpecified(emulatorHost);
        if (!canValidate(isAgainstEmulator)) {
            this.result = false;
            return this;
        }

        String googleCredentials = System.getenv().get(GOOGLE_APPLICATION_CREDENTIALS_ENV_VAR);

        String credentialPath = context.getString(SPANNER_CREDENTIALS_PATH);
        String credentialJson = context.getString(SPANNER_CREDENTIALS_JSON);

        if (!isAgainstEmulator && !FieldValidator.isSpecified(googleCredentials) && !FieldValidator.isSpecified(credentialPath)
                && !FieldValidator.isSpecified(credentialJson)) {
            try {
                ServiceAccountCredentials.getApplicationDefault();
            }
            catch (IOException e) {
                LOGGER.error("The Application Default Credentials are not available.", e);
                this.result = false;
                return this;
            }
            String message = String.format(PLEASE_SPECIFY_CONFIGURATION_PROPERTY_MSG, SPANNER_CREDENTIALS_PATH.name(),
                    SPANNER_CREDENTIALS_JSON.name(), GOOGLE_APPLICATION_CREDENTIALS_ENV_VAR);
            LOGGER.info(message, SPANNER_CREDENTIALS_PATH, SPANNER_CREDENTIALS_JSON);
        }

        if (FieldValidator.isSpecified(host) && isAgainstEmulator) {
            LOGGER.error(HOST_CONFLICT);
            context.error(HOST_CONFLICT, SPANNER_HOST, SPANNER_EMULATOR_HOST);
            result = false;
            return this;
        }

        if (FieldValidator.isSpecified(host) && isAgainstEmulator) {
            LOGGER.error(HOST_CONFLICT);
            context.error(HOST_CONFLICT, SPANNER_HOST, SPANNER_EMULATOR_HOST);
            result = false;
            return this;
        }
        return this;
    }

    @Override
    public boolean isSuccess() {
        return result;
    }

    public boolean canValidate(boolean isAgainstEmulator) {
        // Skip checks for credentials against Emulator.
        return context.getErrors(PROJECT_ID).isEmpty() &&
                context.getErrors(INSTANCE_ID).isEmpty() &&
                context.getErrors(DATABASE_ID).isEmpty() && (isAgainstEmulator || (context.getErrors(SPANNER_CREDENTIALS_JSON).isEmpty() &&
                        context.getErrors(SPANNER_CREDENTIALS_PATH).isEmpty()));
    }

}
