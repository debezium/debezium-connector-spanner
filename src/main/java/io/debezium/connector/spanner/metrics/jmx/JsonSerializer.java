/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.jmx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;

/**
 * Serializes Java objects into JSON
 */
public class JsonSerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonSerializer.class);
    private final ObjectMapper objectMapper;

    public JsonSerializer() {
        this.objectMapper = new ObjectMapper()
                .registerModule(new JSR310Module())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public String writeValueAsString(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        }
        catch (JsonProcessingException ex) {
            LOGGER.warn("Error serialize object: " + obj, ex);
        }
        return "Error serialize object: " + obj;
    }
}
