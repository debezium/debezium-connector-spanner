/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.jmx;

import java.time.Duration;

import org.apache.kafka.connect.data.Struct;

import com.google.cloud.Timestamp;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.connector.spanner.context.source.SpannerSourceTaskContext;
import io.debezium.connector.spanner.metrics.SpannerMeter;
import io.debezium.data.Envelope;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * Implementation of metrics related to the streaming phase of the Spanner connector
 */
public class SpannerStreamingChangeEventSourceMetrics
        extends DefaultStreamingChangeEventSourceMetrics<SpannerPartition> implements SpannerMetricsMXBean {

    private static final String TASK_ID_TAG = "Task";
    private static final String CONNECTOR_NAME_TAG = "ConnectorName";
    private static final String NOT_YET_CALCULATED = "not yet calculated";

    private final SpannerMeter spannerMeter;

    private final JsonSerializer jsonSerializer;

    public SpannerStreamingChangeEventSourceMetrics(SpannerSourceTaskContext taskContext,
                                                    ChangeEventQueueMetrics changeEventQueueMetrics,
                                                    EventMetadataProvider metadataProvider,
                                                    SpannerMeter spannerMeter) {
        super(taskContext, changeEventQueueMetrics, metadataProvider,
                Collect.linkMapOf(CONNECTOR_NAME_TAG, taskContext.getConnectorName(),
                        TASK_ID_TAG, TASK_ID_TAG + "-" + taskContext.getTaskId()));
        this.spannerMeter = spannerMeter;
        this.jsonSerializer = new JsonSerializer();
    }

    @Override
    public void onEvent(SpannerPartition partition, DataCollectionId source, OffsetContext offset, Object key,
                        Struct value, Envelope.Operation operation) {
        super.onEvent(partition, source, offset, key, value, operation);
        spannerMeter.captureTable(source);
    }

    @Override
    public void reset() {
        super.reset();
        spannerMeter.reset();
    }

    @Override
    public String getLowWatermark() throws InterruptedException {
        Timestamp timestamp = spannerMeter.getLowWatermark();
        return timestamp != null ? timestamp.toString() : NOT_YET_CALCULATED;
    }

    @Override
    public Long getMilliSecondsLowWatermark() throws InterruptedException {
        Timestamp timestamp = spannerMeter.getLowWatermark();
        return timestamp != null ? timestamp.toSqlTimestamp().toInstant().toEpochMilli() : null;
    }

    @Override
    public Long getMilliSecondsLowWatermarkLag() throws InterruptedException {
        return spannerMeter.getLowWatermarkLag();
    }

    @Override
    public int getNumberOfChangeStreamPartitionsDetected() {
        return spannerMeter.getNumberOfPartitionsDetected();
    }

    @Override
    public int getNumberOfChangeStreamQueriesIssuedTotal() {
        return spannerMeter.getNumberOfQueriesIssuedTotal();
    }

    @Override
    public int getNumberOfActiveChangeStreamQueries() {
        return spannerMeter.getNumberOfActiveQueries();
    }

    @Override
    public int getStuckHeartbeatIntervals() {
        return spannerMeter.getStuckHeartbeatIntervals();
    }

    @Override
    public Duration getDelayChangeStreamEventsLastMilliSeconds() {
        return spannerMeter.getDelayChangeStreamEvents().getLastValue();
    }

    @Override
    public Double getDelayChangeStreamEventsP50MilliSeconds() {
        return spannerMeter.getDelayChangeStreamEvents().getValueAtP50();
    }

    @Override
    public Double getDelayChangeStreamEventsP95MilliSeconds() {
        return spannerMeter.getDelayChangeStreamEvents().getValueAtP95();
    }

    @Override
    public Double getDelayChangeStreamEventsP99MilliSeconds() {
        return spannerMeter.getDelayChangeStreamEvents().getValueAtP99();
    }

    @Override
    public int getErrorCount() {
        return spannerMeter.getErrorCount();
    }

    @Override
    public String getTaskSyncContext() {
        return jsonSerializer.writeValueAsString(spannerMeter.getTaskSyncContext());
    }

    @Override
    public String getTaskUid() {
        return spannerMeter.getTaskUid();
    }

    // low watermark lag latency

    @Override
    public Long getLatencyLowWatermarkLagMinMilliSeconds() {
        return spannerMeter.getLowWatermarkLagLatency().getMinValue().toMillis();
    }

    @Override
    public Long getLatencyLowWatermarkLagMaxMilliSeconds() {
        return spannerMeter.getLowWatermarkLagLatency().getMaxValue().toMillis();
    }

    @Override
    public Double getLatencyLowWatermarkLagAvgMilliSeconds() {
        return Double.valueOf(spannerMeter.getLowWatermarkLagLatency().getAverageValue().toMillis());
    }

    @Override
    public Double getLatencyLowWatermarkLagP50MilliSeconds() {
        return spannerMeter.getLowWatermarkLagLatency().getValueAtP50();
    }

    @Override
    public Double getLatencyLowWatermarkLagP95MilliSeconds() {
        return spannerMeter.getLowWatermarkLagLatency().getValueAtP95();
    }

    @Override
    public Double getLatencyLowWatermarkLagP99MilliSeconds() {
        return spannerMeter.getLowWatermarkLagLatency().getValueAtP99();
    }

    // Total latency
    @Override
    public Long getLatencyTotalMinMilliSeconds() {
        return spannerMeter.getTotalLatency().getMinValue().toMillis();
    }

    @Override
    public Long getLatencyTotalMaxMilliSeconds() {
        return spannerMeter.getTotalLatency().getMaxValue().toMillis();
    }

    @Override
    public Double getLatencyTotalAvgMilliSeconds() {
        return Double.valueOf(spannerMeter.getTotalLatency().getAverageValue().toMillis());
    }

    @Override
    public Double getLatencyTotalP50MilliSeconds() {
        return spannerMeter.getTotalLatency().getValueAtP50();
    }

    @Override
    public Double getLatencyTotalP95MilliSeconds() {
        return spannerMeter.getTotalLatency().getValueAtP95();
    }

    @Override
    public Double getLatencyTotalP99MilliSeconds() {
        return spannerMeter.getTotalLatency().getValueAtP99();
    }

    // Spanner latency
    @Override
    public Long getLatencySpannerMinMilliSeconds() {
        return spannerMeter.getSpannerLatency().getMinValue().toMillis();
    }

    @Override
    public Long getLatencySpannerMaxMilliSeconds() {
        return spannerMeter.getSpannerLatency().getMaxValue().toMillis();
    }

    @Override
    public Double getLatencySpannerAvgMilliSeconds() {
        return Double.valueOf(spannerMeter.getSpannerLatency().getAverageValue().toMillis());
    }

    @Override
    public Double getLatencySpannerP50MilliSeconds() {
        return spannerMeter.getSpannerLatency().getValueAtP50();
    }

    @Override
    public Double getLatencySpannerP95MilliSeconds() {
        return spannerMeter.getSpannerLatency().getValueAtP95();
    }

    @Override
    public Double getLatencySpannerP99MilliSeconds() {
        return spannerMeter.getSpannerLatency().getValueAtP99();
    }

    // ReadToEmit latency
    @Override
    public Long getLatencyReadToEmitMinMilliSeconds() {
        return spannerMeter.getConnectorLatency().getMinValue().toMillis();
    }

    @Override
    public Long getLatencyReadToEmitMaxMilliSeconds() {
        return spannerMeter.getConnectorLatency().getMaxValue().toMillis();
    }

    @Override
    public Double getLatencyReadToEmitAvgMilliSeconds() {
        return Double.valueOf(spannerMeter.getConnectorLatency().getAverageValue().toMillis());
    }

    @Override
    public Double getLatencyReadToEmitP50MilliSeconds() {
        return spannerMeter.getConnectorLatency().getValueAtP50();
    }

    @Override
    public Double getLatencyReadToEmitP95MilliSeconds() {
        return spannerMeter.getConnectorLatency().getValueAtP95();
    }

    @Override
    public Double getLatencyReadToEmitP99MilliSeconds() {
        return spannerMeter.getConnectorLatency().getValueAtP99();
    }

    // CommitToEmit latency
    @Override
    public Long getLatencyCommitToEmitMinMilliSeconds() {
        return spannerMeter.getCommitToEmitLatency().getMinValue().toMillis();
    }

    @Override
    public Long getLatencyCommitToEmitMaxMilliSeconds() {
        return spannerMeter.getCommitToEmitLatency().getMaxValue().toMillis();
    }

    @Override
    public Double getLatencyCommitToEmitAvgMilliSeconds() {
        return Double.valueOf(spannerMeter.getCommitToEmitLatency().getAverageValue().toMillis());
    }

    @Override
    public Double getLatencyCommitToEmitP50MilliSeconds() {
        return spannerMeter.getCommitToEmitLatency().getValueAtP50();
    }

    @Override
    public Double getLatencyCommitToEmitP95MilliSeconds() {
        return spannerMeter.getCommitToEmitLatency().getValueAtP95();
    }

    @Override
    public Double getLatencyCommitToEmitP99MilliSeconds() {
        return spannerMeter.getCommitToEmitLatency().getValueAtP99();
    }

    // CommitToPublish Latency
    @Override
    public Long getLatencyCommitToPublishMinMilliSeconds() {
        return spannerMeter.getCommitToPublishLatency().getMinValue().toMillis();
    }

    @Override
    public Long getLatencyCommitToPublishMaxMilliSeconds() {
        return spannerMeter.getCommitToPublishLatency().getMaxValue().toMillis();
    }

    @Override
    public Double getLatencyCommitToPublishAvgMilliSeconds() {
        return Double.valueOf(spannerMeter.getCommitToPublishLatency().getAverageValue().toMillis());
    }

    @Override
    public Double getLatencyCommitToPublishP50MilliSeconds() {
        return spannerMeter.getCommitToPublishLatency().getValueAtP50();
    }

    @Override
    public Double getLatencyCommitToPublishP95MilliSeconds() {
        return spannerMeter.getCommitToPublishLatency().getValueAtP95();
    }

    @Override
    public Double getLatencyCommitToPublishP99MilliSeconds() {
        return spannerMeter.getCommitToPublishLatency().getValueAtP99();
    }

    // EmitToPublish Latency
    @Override
    public Long getLatencyEmitToPublishMinMilliSeconds() {
        return spannerMeter.getEmitToPublishLatency().getMinValue().toMillis();
    }

    @Override
    public Long getLatencyEmitToPublishMaxMilliSeconds() {
        return spannerMeter.getEmitToPublishLatency().getMaxValue().toMillis();
    }

    @Override
    public Double getLatencyEmitToPublishAvgMilliSeconds() {
        return Double.valueOf(spannerMeter.getEmitToPublishLatency().getAverageValue().toMillis());
    }

    @Override
    public Double getLatencyEmitToPublishP50MilliSeconds() {
        return spannerMeter.getEmitToPublishLatency().getValueAtP50();
    }

    @Override
    public Double getLatencyEmitToPublishP95MilliSeconds() {
        return spannerMeter.getEmitToPublishLatency().getValueAtP95();
    }

    @Override
    public Double getLatencyEmitToPublishP99MilliSeconds() {
        return spannerMeter.getEmitToPublishLatency().getValueAtP99();
    }

    // debug OwnConnector Latency
    @Override
    public Long getDebugLatencyOwnConnectorMinMilliSeconds() {
        return spannerMeter.getOwnConnectorLatency().getMinValue().toMillis();
    }

    @Override
    public Long getDebugLatencyOwnConnectorMaxMilliSeconds() {
        return spannerMeter.getOwnConnectorLatency().getMaxValue().toMillis();
    }

    @Override
    public Double getDebugLatencyOwnConnectorAvgMilliSeconds() {
        return Double.valueOf(spannerMeter.getOwnConnectorLatency().getAverageValue().toMillis());
    }

    @Override
    public Long getDebugLatencyOwnConnectorLastMilliSeconds() {
        return spannerMeter.getOwnConnectorLatency().getLastValue().toMillis();
    }

    @Override
    public Double getDebugLatencyOwnConnectorP50MilliSeconds() {
        return spannerMeter.getOwnConnectorLatency().getValueAtP50();
    }

    @Override
    public Double getDebugLatencyOwnConnectorP95MilliSeconds() {
        return spannerMeter.getOwnConnectorLatency().getValueAtP95();
    }

    @Override
    public Double getDebugLatencyOwnConnectorP99MilliSeconds() {
        return spannerMeter.getOwnConnectorLatency().getValueAtP99();
    }

    // offset lag statistics

    @Override
    public Long getPartitionOffsetLagMinMilliSeconds() {
        return spannerMeter.getPartitionOffsetLagStatistics().getMinValue().toMillis();
    }

    @Override
    public Long getPartitionOffsetLagMaxMilliSeconds() {
        return spannerMeter.getPartitionOffsetLagStatistics().getMaxValue().toMillis();
    }

    @Override
    public Double getPartitionOffsetLagAvgMilliSeconds() {
        return Double.valueOf(spannerMeter.getPartitionOffsetLagStatistics().getAverageValue().toMillis());
    }

    @Override
    public Double getPartitionOffsetLagP50MilliSeconds() {
        return spannerMeter.getPartitionOffsetLagStatistics().getValueAtP50();
    }

    @Override
    public Double getPartitionOffsetLagP95MilliSeconds() {
        return spannerMeter.getPartitionOffsetLagStatistics().getValueAtP95();
    }

    @Override
    public Double getPartitionOffsetLagP99MilliSeconds() {
        return spannerMeter.getPartitionOffsetLagStatistics().getValueAtP99();
    }

    @Override
    public Long getPartitionOffsetLagLastMilliSeconds() {
        return spannerMeter.getPartitionOffsetLagStatistics().getLastValue().toMillis();
    }

    // offset receiving time statistics
    @Override
    public Long getOffsetReceivingTimeMinMilliSeconds() {
        return spannerMeter.getOffsetReceivingTimeStatistics().getMinValue().toMillis();
    }

    @Override
    public Long getOffsetReceivingTimeMaxMilliSeconds() {
        return spannerMeter.getOffsetReceivingTimeStatistics().getMaxValue().toMillis();
    }

    @Override
    public Double getOffsetReceivingTimeAvgMilliSeconds() {
        return Double.valueOf(spannerMeter.getOffsetReceivingTimeStatistics().getAverageValue().toMillis());
    }

    @Override
    public Double getOffsetReceivingTimeP50MilliSeconds() {
        return spannerMeter.getOffsetReceivingTimeStatistics().getValueAtP50();
    }

    @Override
    public Double getOffsetReceivingTimeP95MilliSeconds() {
        return spannerMeter.getOffsetReceivingTimeStatistics().getValueAtP95();
    }

    @Override
    public Double getOffsetReceivingTimeP99MilliSeconds() {
        return spannerMeter.getOffsetReceivingTimeStatistics().getValueAtP99();
    }

    @Override
    public Long getOffsetReceivingTimeLastMilliSeconds() {
        return spannerMeter.getOffsetReceivingTimeStatistics().getLastValue().toMillis();
    }

    @Override
    public int getSpannerEventQueueTotalCapacity() {
        return spannerMeter.getSpannerEventQueueTotalCapacity();
    }

    @Override
    public int getTaskStateChangeEventQueueRemainingCapacity() {
        return spannerMeter.getTaskStateChangeEventQueueRemainingCapacity();
    }

    @Override
    public int getSpannerEventQueueRemainingCapacity() {
        return spannerMeter.getSpannerEventQueueRemainingCapacity();
    }

    @Override
    public boolean isLeader() {
        return spannerMeter.isLeader();
    }

    @Override
    public long getRebalanceGenerationId() {
        return spannerMeter.getRebalanceGenerationId();
    }

    @Override
    public int getRebalanceAnswersActual() {
        return spannerMeter.getRebalanceAnswersActual();
    }

    @Override
    public int getRebalanceAnswersExpected() {
        return spannerMeter.getRebalanceAnswersExpected();
    }

    @Override
    public void finishTask() {
        spannerMeter.finishTask();
    }

    @Override
    public void restartTask() {
        spannerMeter.restartTask();
    }

}
