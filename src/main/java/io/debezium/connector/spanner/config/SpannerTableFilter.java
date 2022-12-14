/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config;

import java.util.function.Predicate;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.function.Predicates;
import io.debezium.schema.DataCollectionFilters;

/**
 * Checks if database table is included for streaming
 */
public class SpannerTableFilter implements DataCollectionFilters.DataCollectionFilter<TableId> {

    private Predicate<String> includePredicate;
    private Predicate<String> excludePredicate;

    public SpannerTableFilter(SpannerConnectorConfig connectorConfig) {
        if (connectorConfig.tableIncludeList() != null) {
            this.includePredicate = Predicates.includes(connectorConfig.tableIncludeList());
        }
        if (connectorConfig.tableExcludeList() != null) {
            this.excludePredicate = Predicates.excludes(connectorConfig.tableExcludeList());
        }
    }

    @Override
    public boolean isIncluded(TableId tableId) {
        if (includePredicate != null) {
            return includePredicate.test(tableId.getTableName());
        }
        return excludePredicate == null || excludePredicate.test(tableId.getTableName());
    }
}
