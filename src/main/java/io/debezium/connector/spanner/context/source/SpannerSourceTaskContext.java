/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.context.source;

import java.util.Collection;
import java.util.function.Supplier;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Contains contextual information and objects scoped to the lifecycle of Debezium's {@link
 * SourceTask} implementations. Extends {@code CdcSourceTaskContext}
 */
public class SpannerSourceTaskContext extends CdcSourceTaskContext {
    public SpannerSourceTaskContext(
                                    SpannerConnectorConfig config, Supplier<Collection<? extends DataCollectionId>> supplier) {
        super(config.getContextName(), config.getConnectorName(), config.getTaskId(), supplier);
    }
}
