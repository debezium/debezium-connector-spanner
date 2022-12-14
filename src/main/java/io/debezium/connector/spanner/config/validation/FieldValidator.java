/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Validates specific configuration fields
 */
public class FieldValidator {
    private static final Logger LOGGER = getLogger(FieldValidator.class);

    private static final String HEARTBEAT_INTERVAL_LIMITATIONS_MSG = "Heartbeat interval must be between 100 and 300000";

    private static final String FIELD_NOT_SPECIFIED_MSG = "The field is not specified";

    private static final String VALUE_IS_INVALID_MSG = "The '{}' value is invalid: {}";

    private static final String PATH_FIELD_INCORRECT_MSG = "path field is incorrect";

    private static final String JSON_FIELD_DATA_INCORRECT_MSG = "JSON field data is incorrect";

    private static final String INVALID_DATE_MSG = "Invalid date. Should be with format yyyy-MM-dd'T'HH:mm:ssZ";

    private static final String ONLY_OLD_AND_NEW_VALUES_ALLOWED = "Only OLD_AND_NEW_VALUES is allowed";

    private FieldValidator() {
    }

    /**
     * Checks heartbeat interval config parameter
     * @param config Configuration
     * @param field field
     * @param problems validation result store
     * @return 0 if heartbeat interval is correct, 1 if not
     */
    public static int isCorrectHeartBeatInterval(Configuration config, Field field, Field.ValidationOutput problems) {
        long interval = config.getLong(field);
        if (interval > 300000 || interval < 100) {
            problems.accept(field, interval, HEARTBEAT_INTERVAL_LIMITATIONS_MSG);
            return 1;
        }
        return 0;
    }

    /**
     * Checks config parameter is not blank
     * @param config Configuration
     * @param field field
     * @param problems validation result store
     * @return 0 config parameter is not blank, 1 if not
     */
    public static int isNotBlank(Configuration config, Field field, Field.ValidationOutput problems) {
        String value = config.getString(field);
        if (!isSpecified(value)) {
            String errorMsg = FIELD_NOT_SPECIFIED_MSG;
            LOGGER.error(VALUE_IS_INVALID_MSG, field, errorMsg);
            problems.accept(field, value, errorMsg);
            return 1;
        }
        return 0;
    }

    /**
     * Checks config parameter is correct path
     * @param config Configuration
     * @param field field
     * @param problems validation result store
     * @return 0 if path is correct, 1 if not
     */
    public static int isCorrectPath(Configuration config, Field field, Field.ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        File file = new File(value);
        if (!file.exists() || file.isDirectory()) {
            problems.accept(field, value, PATH_FIELD_INCORRECT_MSG);
            return 1;
        }
        return 0;
    }

    /**
     * Checks config parameter is valid json
     * @param config Configuration
     * @param field field
     * @param problems validation result store
     * @return 0 if json is valid, 1 if not
     */
    public static int isCorrectJson(Configuration config, Field field, Field.ValidationOutput problems) {
        String value = config.getString(field);
        if (value == null) {
            return 0;
        }
        if (value.isBlank()) {
            problems.accept(field, value, JSON_FIELD_DATA_INCORRECT_MSG);
            return 1;
        }
        try {
            new ObjectMapper().readTree(value);
        }
        catch (JsonProcessingException e) {
            problems.accept(field, value, JSON_FIELD_DATA_INCORRECT_MSG);
            return 1;
        }
        return 0;
    }

    /**
     * Checks config parameter is valid capture mode
     * @param config Configuration
     * @param field field
     * @param problems validation result store
     * @return 0 if capture mode is valid, 1 if not
     */
    public static int isCorrectCaptureMode(Configuration config, Field field, Field.ValidationOutput problems) {
        String value = config.getString(field);
        if (value != null && !"OLD_AND_NEW_VALUES".equals(value)) {
            String errorMsg = ONLY_OLD_AND_NEW_VALUES_ALLOWED;
            LOGGER.error(VALUE_IS_INVALID_MSG, field, errorMsg);
            problems.accept(field, value, errorMsg);
            return 1;
        }
        return 0;
    }

    /**
     * Checks config parameter is valid timestamp
     * @param config Configuration
     * @param field field
     * @param problems validation result store
     * @return 0 if timestamp is valid, 1 if not
     */
    public static int isCorrectDateTime(Configuration config, Field field, Field.ValidationOutput problems) {
        String dateTime = config.getString(field);
        if (dateTime == null) {
            return 0;
        }
        try {
            DateTimeFormatter.ISO_INSTANT.parse(dateTime);
        }
        catch (Exception ex) {
            problems.accept(field, dateTime, INVALID_DATE_MSG);
            return 1;
        }
        return 0;
    }

    /**
     * Checks string is not null and not blank
     * @param value string
     * @return 0 if string is specified, 1 if not
     */
    public static boolean isSpecified(String value) {
        return !Objects.isNull(value) && !value.isBlank();
    }
}
