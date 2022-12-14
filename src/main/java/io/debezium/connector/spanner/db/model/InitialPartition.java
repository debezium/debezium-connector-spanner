/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import java.util.Objects;

/**
 * Utility class to determine initial partition constants and methods.
 *
 * <p>The initial partition has the artificial token defined by {@link
 * InitialPartition#PARTITION_TOKEN} and it has no parent tokens.
 */
public class InitialPartition {

    private InitialPartition() {
    }

    /**
     * The token of the initial partition. This is an artificial token for the Connector and it is not
     * recognised by Cloud Spanner.
     */
    public static final String PARTITION_TOKEN = "Parent0";

    /**
     * Verifies if the given partition token is the initial partition.
     *
     * @param partitionToken the partition token to be checked
     * @return true if the given token is the initial partition, and false otherwise
     */
    public static boolean isInitialPartition(String partitionToken) {
        return Objects.equals(PARTITION_TOKEN, partitionToken);
    }

}
