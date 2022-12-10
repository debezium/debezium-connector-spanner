/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal.model;

/**
 * DTO for transferring Sync Event information,
 * between application layers.
 */
public class SyncEventMetadata {
    long offset;
    boolean canInitiateRebalancing;

    SyncEventMetadata(final long offset, final boolean canInitiateRebalancing) {
        this.offset = offset;
        this.canInitiateRebalancing = canInitiateRebalancing;
    }

    public static class SyncEventMetadataBuilder {

        private long offset;

        private boolean canInitiateRebalancing;

        SyncEventMetadataBuilder() {
        }

        public SyncEventMetadata.SyncEventMetadataBuilder offset(final long offset) {
            this.offset = offset;
            return this;
        }

        public SyncEventMetadata.SyncEventMetadataBuilder canInitiateRebalancing(final boolean canInitiateRebalancing) {
            this.canInitiateRebalancing = canInitiateRebalancing;
            return this;
        }

        public SyncEventMetadata build() {
            return new SyncEventMetadata(this.offset, this.canInitiateRebalancing);
        }
    }

    public static SyncEventMetadata.SyncEventMetadataBuilder builder() {
        return new SyncEventMetadata.SyncEventMetadataBuilder();
    }

    public long getOffset() {
        return this.offset;
    }

    public boolean isCanInitiateRebalancing() {
        return this.canInitiateRebalancing;
    }

    public void setOffset(final long offset) {
        this.offset = offset;
    }

    public void setCanInitiateRebalancing(final boolean canInitiateRebalancing) {
        this.canInitiateRebalancing = canInitiateRebalancing;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof SyncEventMetadata)) {
            return false;
        }
        final SyncEventMetadata other = (SyncEventMetadata) o;
        if (!other.canEqual((Object) this)) {
            return false;
        }
        if (this.getOffset() != other.getOffset()) {
            return false;
        }
        if (this.isCanInitiateRebalancing() != other.isCanInitiateRebalancing()) {
            return false;
        }
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof SyncEventMetadata;
    }

    @Override
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final long offsetValue = this.getOffset();
        result = result * PRIME + (int) (offsetValue >>> 32 ^ offsetValue);
        result = result * PRIME + (this.isCanInitiateRebalancing() ? 79 : 97);
        return result;
    }

    @Override
    public String toString() {
        return "SyncEventMetadata(offset=" + this.getOffset() +
                ", canInitiateRebalancing=" + this.isCanInitiateRebalancing() + ")";
    }
}
