/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import java.util.function.Supplier;

import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Immutable value object that bundles all mutable key range streaming options for
 * {@link SpannerChangeStreamService}.  Using a single options object instead of an
 * ever-growing constructor parameter list keeps callers readable and makes it trivial
 * to add future tuning knobs without another constructor overload.
 *
 * <p>Obtain instances via the static factory methods:
 * <ul>
 *   <li>{@link #withDefaults()} — ordering enabled, no buffer-gate supplier (falls back
 *       to close/reopen on every MoveIn)</li>
 *   <li>{@link #orderingDisabled()} — MoveIn/MoveOut ordering skipped entirely</li>
 *   <li>{@link #of} — ordering enabled with a live buffer-gate supplier and explicit tuning</li>
 * </ul>
 */
public final class MutableStreamOptions {

    /** Default maximum events the {@link MoveInBufferGate} may hold before overflowing. */
    static final int DEFAULT_BUFFER_MAX_EVENTS = 5000;

    /** Default polling interval (ms) for the post-window spin-wait. */
    static final int DEFAULT_GATE_CHECK_INTERVAL_MS = 10;

    /** Default maximum time (ms) the post-window spin-wait may block before falling back. */
    static final int DEFAULT_GATE_TIMEOUT_MS = 60_000;

    private final boolean orderingEnabled;
    private final Supplier<TaskSyncContext> taskSyncContextSupplier;
    private final int bufferMaxEvents;
    private final int gateCheckIntervalMs;
    private final int gateTimeoutMs;

    private MutableStreamOptions(boolean orderingEnabled,
                                 Supplier<TaskSyncContext> taskSyncContextSupplier,
                                 int bufferMaxEvents,
                                 int gateCheckIntervalMs,
                                 int gateTimeoutMs) {
        this.orderingEnabled = orderingEnabled;
        this.taskSyncContextSupplier = taskSyncContextSupplier;
        this.bufferMaxEvents = bufferMaxEvents;
        this.gateCheckIntervalMs = gateCheckIntervalMs;
        this.gateTimeoutMs = gateTimeoutMs;
    }

    /**
     * Ordering enabled with all buffer-gate parameters at their defaults.
     * The buffer gate is not active (supplier is {@code null}), so the close/reopen
     * path is used for every MoveIn event.
     */
    public static MutableStreamOptions withDefaults() {
        return new MutableStreamOptions(true, null,
                DEFAULT_BUFFER_MAX_EVENTS, DEFAULT_GATE_CHECK_INTERVAL_MS, DEFAULT_GATE_TIMEOUT_MS);
    }

    /**
     * Ordering disabled — MoveIn/MoveOut events are forwarded to the consumer immediately
     * without any gate or sequencing check.
     */
    public static MutableStreamOptions orderingDisabled() {
        return new MutableStreamOptions(false, null,
                DEFAULT_BUFFER_MAX_EVENTS, DEFAULT_GATE_CHECK_INTERVAL_MS, DEFAULT_GATE_TIMEOUT_MS);
    }

    /**
     * Ordering enabled with the buffer-gate optimisation fully wired.
     *
     * @param taskSyncContextSupplier non-blocking supplier of the current {@link TaskSyncContext};
     *                                must not be {@code null}
     * @param bufferMaxEvents         maximum events the gate may hold before overflowing
     * @param gateCheckIntervalMs     polling interval (ms) for the post-window spin-wait
     * @param gateTimeoutMs           maximum spin-wait duration (ms) before falling back
     */
    public static MutableStreamOptions of(Supplier<TaskSyncContext> taskSyncContextSupplier,
                                          int bufferMaxEvents,
                                          int gateCheckIntervalMs,
                                          int gateTimeoutMs) {
        return new MutableStreamOptions(true, taskSyncContextSupplier,
                bufferMaxEvents, gateCheckIntervalMs, gateTimeoutMs);
    }

    public boolean isOrderingEnabled() {
        return orderingEnabled;
    }

    public Supplier<TaskSyncContext> getTaskSyncContextSupplier() {
        return taskSyncContextSupplier;
    }

    public int getBufferMaxEvents() {
        return bufferMaxEvents;
    }

    public int getGateCheckIntervalMs() {
        return gateCheckIntervalMs;
    }

    public int getGateTimeoutMs() {
        return gateTimeoutMs;
    }
}
