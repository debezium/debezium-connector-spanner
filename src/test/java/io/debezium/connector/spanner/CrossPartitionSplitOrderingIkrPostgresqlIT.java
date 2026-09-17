/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Dialect;

import io.debezium.connector.spanner.util.PartitionMode;

public class CrossPartitionSplitOrderingIkrPostgresqlIT extends CrossPartitionSplitOrderingTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(CrossPartitionSplitOrderingIkrPostgresqlIT.class);

    @Test
    public void shouldDeliverFollowUpWriteOnceInOrderAcrossBackgroundPartitionSplitsIkrPostgresql()
            throws InterruptedException {
        shouldDeliverFollowUpWriteOnceInOrderAcrossBackgroundPartitionSplits(
                PartitionMode.IMMUTABLE_KEY_RANGE, Dialect.POSTGRESQL, LOGGER);
    }
}
