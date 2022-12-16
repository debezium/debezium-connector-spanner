/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.spanner.context.offset.SpannerOffsetContext;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.schema.DatabaseSchema;

/**
 * Coordinates Spanner ChangeEventSource to execute them in order
 */
public class SpannerChangeEventSourceCoordinator extends ChangeEventSourceCoordinator<SpannerPartition, SpannerOffsetContext> {

    public SpannerChangeEventSourceCoordinator(Offsets previousOffsets,
                                               ErrorHandler errorHandler,
                                               Class connectorType,
                                               CommonConnectorConfig connectorConfig,
                                               ChangeEventSourceFactory changeEventSourceFactory,
                                               ChangeEventSourceMetricsFactory changeEventSourceMetricsFactory,
                                               EventDispatcher eventDispatcher,
                                               DatabaseSchema schema) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema);
    }

    public void commitRecords(List<SourceRecord> recordList) throws InterruptedException {
        if (this.streamingSource instanceof CommittingRecordsStreamingChangeEventSource) {
            SpannerStreamingChangeEventSource streamingChangeEventSource = (SpannerStreamingChangeEventSource) this.streamingSource;
            streamingChangeEventSource.commitRecords(recordList);
        }
    }

}
