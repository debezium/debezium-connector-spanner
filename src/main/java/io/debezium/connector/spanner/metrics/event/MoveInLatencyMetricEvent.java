/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.event;

/**
 * Breaks down the latency observed when a mutable partition pauses on a MoveIn
 * boundary event into the stages of the read that produced it, so that the
 * dashboard can distinguish "the connector hadn't issued the next query yet"
 * (backlog, actionable in the connector) from "Spanner held the row after the
 * query/stream was already open" (upstream, not actionable in the connector).
 */
public class MoveInLatencyMetricEvent implements MetricEvent {

    private final long commitToQueryMillis;

    private final long queryToStreamStartMillis;

    private final long streamStartToReadMillis;

    private final long commitToReadMillis;

    public MoveInLatencyMetricEvent(long commitToQueryMillis, long queryToStreamStartMillis, long streamStartToReadMillis,
                                    long commitToReadMillis) {
        this.commitToQueryMillis = commitToQueryMillis;
        this.queryToStreamStartMillis = queryToStreamStartMillis;
        this.streamStartToReadMillis = streamStartToReadMillis;
        this.commitToReadMillis = commitToReadMillis;
    }

    /**
     * Staleness of the record before the connector even issued the query that read it.
     * Large values indicate connector-side backlog from repeatedly reopening the
     * query on every boundary, not a Spanner delivery problem.
     */
    public long getCommitToQueryMillis() {
        return commitToQueryMillis;
    }

    /**
     * Time between issuing the query and the underlying stream starting to return
     * bytes. Reflects RPC/query setup overhead.
     */
    public long getQueryToStreamStartMillis() {
        return queryToStreamStartMillis;
    }

    /**
     * Time spent waiting inside an already-open stream before this specific row
     * became available. Large values here, with a small commitToQueryMillis,
     * would indicate a genuine Spanner-side delivery delay.
     */
    public long getStreamStartToReadMillis() {
        return streamStartToReadMillis;
    }

    /**
     * Total staleness at read time: commitToQueryMillis + queryToStreamStartMillis +
     * streamStartToReadMillis.
     */
    public long getCommitToReadMillis() {
        return commitToReadMillis;
    }
}
