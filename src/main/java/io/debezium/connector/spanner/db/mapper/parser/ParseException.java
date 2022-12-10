/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.mapper.parser;

/**
 * Exception thrown while parsing json string in DTOs classes
 */
public class ParseException extends RuntimeException {
    public ParseException(String json, Exception ex) {
        super("Error parse string: " + json, ex);
    }
}
