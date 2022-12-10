/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.jmx;

import javax.management.MXBean;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

/**
 * Spanner metrics which are available on JMX
 */
@MXBean
public interface SpannerMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {

    /**
     * The low watermark: The timestamp T at which we have already received records with timestamp < T
     */
    String getLowWatermark() throws InterruptedException;

    /**
     * The low watermark converted to the number of milliseconds from the epoch of 1970-01-01T00:00:00Z
     */
    Long getMilliSecondsLowWatermark() throws InterruptedException;

    /**
     * The difference between the current clock time and the low watermark
     */
    Long getMilliSecondsLowWatermarkLag() throws InterruptedException;

    Long getLatencyLowWatermarkLagMinMilliSeconds();

    Long getLatencyLowWatermarkLagMaxMilliSeconds();

    Double getLatencyLowWatermarkLagAvgMilliSeconds();

    Double getLatencyLowWatermarkLagP50MilliSeconds();

    Double getLatencyLowWatermarkLagP95MilliSeconds();

    Double getLatencyLowWatermarkLagP99MilliSeconds();

    /**
     * The total number of partitions detected by the current task
     */
    int getNumberOfChangeStreamPartitionsDetected();

    /**
     * The total number of issued queries in the current task
     */
    int getNumberOfChangeStreamQueriesIssuedTotal();

    /**
     * The number of active partitions in the current task
     */
    int getNumberOfActiveChangeStreamQueries();

    /**
     * The number of intervals waiting but not receiving Heartbeat
     * records from the Spanner Change Stream
     */
    int getStuckHeartbeatIntervals();

    /**
     * The delay which Spanner connector waits for
     * the next Change Stream Event
     */
    Long getDelayChangeStreamEventsLastMilliSeconds();

    Double getDelayChangeStreamEventsP50MilliSeconds();

    Double getDelayChangeStreamEventsP95MilliSeconds();

    Double getDelayChangeStreamEventsP99MilliSeconds();

    /**
     * The total number of Runtime errors
     */
    int getErrorCount();

    /**
     * The internal state of the task
     */
    String getTaskSyncContext();

    /**
     * Unique identifier for the current task
     */
    String getTaskUid();

    /**
     * The Total Latency of the Spanner Connector
     */
    Long getLatencyTotalMinMilliSeconds();

    Long getLatencyTotalMaxMilliSeconds();

    Double getLatencyTotalAvgMilliSeconds();

    Double getLatencyTotalP50MilliSeconds();

    Double getLatencyTotalP95MilliSeconds();

    Double getLatencyTotalP99MilliSeconds();

    /**
     * Spanner commit to Connector read latency
     */
    Long getLatencySpannerMinMilliSeconds();

    Long getLatencySpannerMaxMilliSeconds();

    Double getLatencySpannerAvgMilliSeconds();

    Double getLatencySpannerP50MilliSeconds();

    Double getLatencySpannerP95MilliSeconds();

    Double getLatencySpannerP99MilliSeconds();

    /**
     * Connector read latency to Connector emit latency
     */
    Long getLatencyReadToEmitMinMilliSeconds();

    Long getLatencyReadToEmitMaxMilliSeconds();

    Double getLatencyReadToEmitAvgMilliSeconds();

    Double getLatencyReadToEmitP50MilliSeconds();

    Double getLatencyReadToEmitP95MilliSeconds();

    Double getLatencyReadToEmitP99MilliSeconds();

    /**
     * Spanner commit to Connector emit latency
     */
    Long getLatencyCommitToEmitMinMilliSeconds();

    Long getLatencyCommitToEmitMaxMilliSeconds();

    Double getLatencyCommitToEmitAvgMilliSeconds();

    Double getLatencyCommitToEmitP50MilliSeconds();

    Double getLatencyCommitToEmitP95MilliSeconds();

    Double getLatencyCommitToEmitP99MilliSeconds();

    /**
     * Spanner commit to Kafka publish latency
     */
    Long getLatencyCommitToPublishMinMilliSeconds();

    Long getLatencyCommitToPublishMaxMilliSeconds();

    Double getLatencyCommitToPublishAvgMilliSeconds();

    Double getLatencyCommitToPublishP50MilliSeconds();

    Double getLatencyCommitToPublishP95MilliSeconds();

    Double getLatencyCommitToPublishP99MilliSeconds();

    /**
     * Connector emit to Kafka publish latency
     */
    Long getLatencyEmitToPublishMinMilliSeconds();

    Long getLatencyEmitToPublishMaxMilliSeconds();

    Double getLatencyEmitToPublishAvgMilliSeconds();

    Double getLatencyEmitToPublishP50MilliSeconds();

    Double getLatencyEmitToPublishP95MilliSeconds();

    Double getLatencyEmitToPublishP99MilliSeconds();

    /**
     * Own latency of the Spanner Connector
     */
    Long getDebugLatencyOwnConnectorMinMilliSeconds();

    Long getDebugLatencyOwnConnectorMaxMilliSeconds();

    Double getDebugLatencyOwnConnectorAvgMilliSeconds();

    Long getDebugLatencyOwnConnectorLastMilliSeconds();

    Double getDebugLatencyOwnConnectorP50MilliSeconds();

    Double getDebugLatencyOwnConnectorP95MilliSeconds();

    Double getDebugLatencyOwnConnectorP99MilliSeconds();

    /**
     * The time difference between now and the start of the partition
     */
    Long getPartitionOffsetLagMinMilliSeconds();

    Long getPartitionOffsetLagMaxMilliSeconds();

    Double getPartitionOffsetLagAvgMilliSeconds();

    Double getPartitionOffsetLagP50MilliSeconds();

    Double getPartitionOffsetLagP95MilliSeconds();

    Double getPartitionOffsetLagP99MilliSeconds();

    Long getPartitionOffsetLagLastMilliSeconds();

    /**
     * The time duration between requesting and receiving
     * offsets from Kafka Connect
     */
    Long getOffsetReceivingTimeMinMilliSeconds();

    Long getOffsetReceivingTimeMaxMilliSeconds();

    Double getOffsetReceivingTimeAvgMilliSeconds();

    Double getOffsetReceivingTimeP50MilliSeconds();

    Double getOffsetReceivingTimeP95MilliSeconds();

    Double getOffsetReceivingTimeP99MilliSeconds();

    Long getOffsetReceivingTimeLastMilliSeconds();

    /**
     * The total capacity of the Spanner Queue
     */
    int getSpannerEventQueueTotalCapacity();

    /**
     * The remaining capacity of the change stream event Queue
     */
    int getSpannerEventQueueRemainingCapacity();

    /**
     * The remaining capacity of the task state change event queue
     */
    int getTaskStateChangeEventQueueRemainingCapacity();

    /**
     * Returns "true", if task is currently acting as a leader
     */
    boolean isLeader();

    long getRebalanceGenerationId();

    /**
     * The number of actual responses during the last Rebalance Event
     */
    int getRebalanceAnswersActual();

    /**
     * The number of expected responses during the last Rebalance Event
     */
    int getRebalanceAnswersExpected();

    /**
     * This method finishes the current task
     */
    void finishTask();

    /**
     * This method restarts the current task
     */
    void restartTask();
}
