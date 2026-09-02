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

public class CrossPartitionSplitOrderingMkrGoogleSqlIT extends CrossPartitionSplitOrderingTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(CrossPartitionSplitOrderingMkrGoogleSqlIT.class);

    @Test
    public void shouldDeliverFollowUpWriteOnceInOrderAcrossBackgroundPartitionSplitsMkrGoogleSql()
            throws InterruptedException {
        shouldDeliverFollowUpWriteOnceInOrderAcrossBackgroundPartitionSplits(
                PartitionMode.MUTABLE_KEY_RANGE, Dialect.GOOGLE_STANDARD_SQL, LOGGER);
    }
}
