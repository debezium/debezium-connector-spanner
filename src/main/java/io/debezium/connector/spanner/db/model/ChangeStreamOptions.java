/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

public final class ChangeStreamOptions {

    private final boolean mutableKeyRange;
    private final boolean perPlacementTvf;

    public ChangeStreamOptions(boolean mutableKeyRange, boolean perPlacementTvf) {
        this.mutableKeyRange = mutableKeyRange;
        this.perPlacementTvf = perPlacementTvf;
    }

    public boolean isMutableKeyRange() {
        return mutableKeyRange;
    }

    public boolean isPerPlacementTvf() {
        return perPlacementTvf;
    }
}
