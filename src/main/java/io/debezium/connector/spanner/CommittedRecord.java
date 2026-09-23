/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

public record CommittedRecord(String token, String tvfName, String recordUid) {
    public CommittedRecord(String token, String recordUid) {
        this(token, null, recordUid);
    }
}
