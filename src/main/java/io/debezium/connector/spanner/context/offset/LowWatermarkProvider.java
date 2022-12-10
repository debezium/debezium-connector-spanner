/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.context.offset;

import com.google.cloud.Timestamp;

/**
 * The {@code LowWatermarkProvider} interface should be implemented by class
 * that calculates low watermark
 */
public interface LowWatermarkProvider {

    /**
     * Returns a watermark, may block a thread
     * @return Timestamp
     *         low watermark
     * @throws InterruptedException
     *         if any thread has interrupted the current thread.
     */
    Timestamp getLowWatermark() throws InterruptedException;
}
