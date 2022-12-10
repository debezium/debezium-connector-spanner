/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.mapper.parser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.db.model.schema.ColumnType;
import io.debezium.connector.spanner.db.model.schema.DataType;

class ColumnTypeParserTest {

    @Test
    void parse() {
        ColumnType typeCode = ColumnTypeParser.parse("{\"array_element_type\":{\"code\":\"BOOL\"},\"code\":\"ARRAY\"}");

        Assertions.assertEquals(DataType.ARRAY, typeCode.getType());
        Assertions.assertEquals(DataType.BOOL, typeCode.getArrayElementType().getType());
    }
}
