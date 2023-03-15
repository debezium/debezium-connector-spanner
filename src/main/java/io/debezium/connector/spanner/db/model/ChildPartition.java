/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import java.util.Objects;
import java.util.Set;

/**
 * A child partition represents a new partition that should be queried.
 */
public class ChildPartition {

    private final String token;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChildPartition that = (ChildPartition) o;
        return Objects.equals(token, that.token) && Objects.equals(parentTokens, that.parentTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, parentTokens);
    }

    private final Set<String> parentTokens;

    /**
     * Constructs a child partition, which will have its own token and the parents that it originated
     * from. A child partition will have a single parent if it is originated from a partition move or
     * split. A child partition will have multiple parents if it is originated from a partition merge.
     *
     * @param token        the child partition token
     * @param parentTokens the partition tokens of the parent(s) that originated the child partition
     */
    public ChildPartition(String token, Set<String> parentTokens) {
        this.token = token;
        this.parentTokens = parentTokens;
    }

    /**
     * Unique partition identifier, which can be used to perform a change stream query.
     *
     * @return the unique partition identifier
     */
    public String getToken() {
        return token;
    }

    /**
     * The unique partition identifiers of the parent partitions where this child partition originated
     * from.
     *
     * @return a set of parent partition tokens
     */
    public Set<String> getParentTokens() {
        return parentTokens;
    }

    @Override
    public String toString() {
        return "ChildPartition{"
                + "childToken='"
                + token
                + '\''
                + ", parentTokens="
                + parentTokens
                + '}';
    }
}
