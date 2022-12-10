/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.utils;

import java.time.Duration;
import java.time.Instant;

/**
 * Utility for waiting a specified duration of time
 */
public class TimeoutMeter {

    private final Duration duration;

    private final Instant instant;

    private TimeoutMeter(Duration duration) {
        this.duration = duration;
        this.instant = Instant.now();
    }

    public static TimeoutMeter setTimeout(Duration duration) {
        return new TimeoutMeter(duration);
    }

    public boolean isExpired() {
        return Duration.between(instant, Instant.now()).compareTo(duration) >= 0;
    }

}