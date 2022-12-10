/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.mapper.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.spanner.db.model.schema.ColumnType;

/**
 * Helper for parsing column data
 */
public class ColumnTypeParser {
    private static final ObjectMapper mapper = new ObjectMapper();

    private ColumnTypeParser() {
    }

    public static ColumnType parse(String json) {
        try {
            return mapper.readValue(json, ColumnType.class);
        }
        catch (JsonProcessingException e) {
            throw new ParseException(json, e);
        }
    }
}
