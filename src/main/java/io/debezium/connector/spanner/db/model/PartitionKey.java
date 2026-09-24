/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import java.util.Objects;

/**
 * Identifies a Spanner partition within its placement TVF token space.
 */
public final class PartitionKey implements Comparable<PartitionKey> {

    private final String token;
    private final String tvfName;

    public PartitionKey(String token, String tvfName) {
        this.token = Objects.requireNonNull(token);
        this.tvfName = tvfName == null || tvfName.isBlank() ? null : tvfName;
    }

    public String getToken() {
        return token;
    }

    public String getTvfName() {
        return tvfName;
    }

    @Override
    public int compareTo(PartitionKey other) {
        int tokenComparison = token.compareTo(other.token);
        if (tokenComparison != 0) {
            return tokenComparison;
        }
        if (tvfName == null) {
            return other.tvfName == null ? 0 : -1;
        }
        return other.tvfName == null ? 1 : tvfName.compareTo(other.tvfName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionKey that = (PartitionKey) o;
        return token.equals(that.token) && Objects.equals(tvfName, that.tvfName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, tvfName);
    }

    @Override
    public String toString() {
        return "PartitionKey{" + "token='" + token + '\'' + ", tvfName='" + tvfName + '\'' + '}';
    }
}
