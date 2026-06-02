/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

public class CommittedRecord {
    private final String token;
    private final String recordUid;

    public CommittedRecord(String token, String recordUid) {
        this.token = token;
        this.recordUid = recordUid;
    }

    public String getToken() {
        return token;
    }

    public String getRecordUid() {
        return recordUid;
    }
}
