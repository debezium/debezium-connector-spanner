/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import java.util.concurrent.atomic.AtomicReference;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.context.offset.LowWatermarkProvider;

/**
 * A wrapper class for watermark data
 */
public class LowWatermarkHolder implements LowWatermarkProvider {

    private final AtomicReference<Timestamp> lastWatermark = new AtomicReference<>(null);

    @Override
    public Timestamp getLowWatermark() throws InterruptedException {
        while (lastWatermark.get() == null) {
            Thread.sleep(1);
        }
        return lastWatermark.get();
    }

    public void setLowWatermark(Timestamp lowWatermark) {
        this.lastWatermark.set(lowWatermark);
    }

}
