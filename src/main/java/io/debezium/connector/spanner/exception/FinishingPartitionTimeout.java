/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.exception;

import java.util.List;

public class FinishingPartitionTimeout extends SpannerConnectorException {
    public FinishingPartitionTimeout(List<String> tokens) {
        super("Partitions awaiting finish : " + tokens);
    }
}
