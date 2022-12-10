/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.db.metadata.TableId;

class SpannerTableFilterTest {

    @Test
    void testConstructor() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        SpannerTableFilter actualSpannerTableFilter = new SpannerTableFilter(new SpannerConnectorConfig(configuration));
        boolean actualIsIncludedResult = actualSpannerTableFilter.isIncluded(TableId.getTableId("Table Name"));
        assertTrue(actualIsIncludedResult);
    }

    @Test
    void isIncludedWithExcludeList() {
        SpannerConnectorConfig connectorConfig = Mockito.mock(SpannerConnectorConfig.class);
        Mockito.when(connectorConfig.tableExcludeList()).thenReturn("TestTable");
        Mockito.when(connectorConfig.tableIncludeList()).thenReturn(null);

        SpannerTableFilter spannerTableFilter = new SpannerTableFilter(connectorConfig);

        boolean included = spannerTableFilter.isIncluded(TableId.getTableId("TestTable"));

        Assertions.assertFalse(included);

        included = spannerTableFilter.isIncluded(TableId.getTableId("Table"));

        Assertions.assertTrue(included);

    }

    @Test
    void isIncludedWithIncludeList() {
        SpannerConnectorConfig connectorConfig = Mockito.mock(SpannerConnectorConfig.class);
        Mockito.when(connectorConfig.tableExcludeList()).thenReturn(null);
        Mockito.when(connectorConfig.tableIncludeList()).thenReturn("TestTable");

        SpannerTableFilter spannerTableFilter = new SpannerTableFilter(connectorConfig);

        boolean included = spannerTableFilter.isIncluded(TableId.getTableId("TestTable"));

        Assertions.assertTrue(included);

        included = spannerTableFilter.isIncluded(TableId.getTableId("Table"));

        Assertions.assertFalse(included);
    }

    @Test
    void isIncludedNoLists() {
        SpannerConnectorConfig connectorConfig = Mockito.mock(SpannerConnectorConfig.class);
        Mockito.when(connectorConfig.tableExcludeList()).thenReturn(null);
        Mockito.when(connectorConfig.tableIncludeList()).thenReturn(null);

        SpannerTableFilter spannerTableFilter = new SpannerTableFilter(connectorConfig);

        boolean included = spannerTableFilter.isIncluded(TableId.getTableId("TestTable"));

        Assertions.assertTrue(included);

        included = spannerTableFilter.isIncluded(TableId.getTableId("Table"));

        Assertions.assertTrue(included);
    }

    @Test
    void isIncludedWithRegExpExpressions() {
        SpannerConnectorConfig connectorConfig = Mockito.mock(SpannerConnectorConfig.class);
        Mockito.when(connectorConfig.tableExcludeList()).thenReturn(null);
        Mockito.when(connectorConfig.tableIncludeList()).thenReturn(".*Account");

        SpannerTableFilter spannerTableFilter = new SpannerTableFilter(connectorConfig);

        boolean included = spannerTableFilter.isIncluded(TableId.getTableId("userAccount"));

        Assertions.assertTrue(included);

        included = spannerTableFilter.isIncluded(TableId.getTableId("userInfo"));

        Assertions.assertFalse(included);
    }

    @Test
    void testIsIncluded() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        SpannerTableFilter spannerTableFilter = new SpannerTableFilter(new SpannerConnectorConfig(configuration));
        assertTrue(spannerTableFilter.isIncluded(TableId.getTableId("Table Name")));
    }
}
