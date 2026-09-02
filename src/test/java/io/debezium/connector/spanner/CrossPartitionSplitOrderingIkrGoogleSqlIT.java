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

public class CrossPartitionSplitOrderingIkrGoogleSqlIT extends CrossPartitionSplitOrderingTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(CrossPartitionSplitOrderingIkrGoogleSqlIT.class);

    @Test
    public void shouldDeliverFollowUpWriteOnceInOrderAcrossBackgroundPartitionSplitsIkrGoogleSql()
            throws InterruptedException {
        shouldDeliverFollowUpWriteOnceInOrderAcrossBackgroundPartitionSplits(
                PartitionMode.IMMUTABLE_KEY_RANGE, Dialect.GOOGLE_STANDARD_SQL, LOGGER);
    }
}
