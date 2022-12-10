/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.spanner.db.mapper.parser.ParseException;

/**
 * Helper for parsing JSON data
 */
public class MapperUtils {

    private static final ObjectMapper mapper = new ObjectMapper();

    private MapperUtils() {
    }

    public static JsonNode getJsonNode(String json) {
        if (json == null) {
            return mapper.createObjectNode();
        }
        try {
            return mapper.readTree(json);
        }
        catch (JsonProcessingException e) {
            throw new ParseException(json, e);
        }
    }
}
