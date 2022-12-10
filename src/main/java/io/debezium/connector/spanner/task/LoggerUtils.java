/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.apache.commons.lang3.ClassUtils.isPrimitiveOrWrapper;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Utility for logging objects in JSON format
 */
public class LoggerUtils {

    private static final LoggerUtils INSTANCE = new LoggerUtils();

    private final AtomicBoolean isJsonLogEnabled;
    private final ObjectWriter objectWriter;

    private LoggerUtils() {
        isJsonLogEnabled = new AtomicBoolean(false);
        objectWriter = new ObjectMapper()
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .writerWithDefaultPrettyPrinter();
    }

    private void setJsonLogEnabled(boolean isJsonLogEnabled) {
        this.isJsonLogEnabled.set(isJsonLogEnabled);
    }

    public static void enableJsonLog() {
        INSTANCE.setJsonLogEnabled(true);
    }

    public static void debug(Logger logger, String message, Object... objects) {
        if (!logger.isDebugEnabled()) {
            return;
        }
        if (INSTANCE.isJsonLogEnabled()) {
            String[] strings = Arrays.stream(objects)
                    .map(o -> isNotObject(o) ? toString(o) : toPrettyJson(o))
                    .toArray(String[]::new);
            logger.debug(message, strings);
        }
        else {
            logger.debug(message, objects);
        }
    }

    private boolean isJsonLogEnabled() {
        return this.isJsonLogEnabled.get();
    }

    private ObjectWriter getObjectWriter() {
        return objectWriter;
    }

    private static String toPrettyJson(Object object) {
        try {
            return INSTANCE.getObjectWriter().writeValueAsString(object);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static String toString(Object object) {
        return object == null ? "null" : object.toString();
    }

    private static boolean isNotObject(Object object) {
        return object == null
                || isPrimitiveOrWrapper(object.getClass())
                || object instanceof String;
    }
}
