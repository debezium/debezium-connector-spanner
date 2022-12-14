/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

/**
 * A Wrapper class for epoch offset
 */
public class EpochOffsetHolder {

    private final long epochOffset;

    public EpochOffsetHolder(long epochOffset) {
        this.epochOffset = epochOffset;
    }

    public long getEpochOffset() {
        return epochOffset;
    }

    public EpochOffsetHolder nextOffset(long offset) {
        return new EpochOffsetHolder(offset);
    }
}
