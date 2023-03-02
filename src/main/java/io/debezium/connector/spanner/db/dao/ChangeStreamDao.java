/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import io.debezium.connector.spanner.db.model.InitialPartition;

/**
 * Executes streaming queries to the Spanner database
 */
public class ChangeStreamDao {

    private final String changeStreamName;
    private final DatabaseClient databaseClient;
    private final RpcPriority rpcPriority;
    private final String jobName;

    public ChangeStreamDao(String changeStreamName, DatabaseClient databaseClient, RpcPriority rpcPriority,
                           String jobName) {
        this.changeStreamName = changeStreamName;
        this.databaseClient = databaseClient;
        this.rpcPriority = rpcPriority;
        this.jobName = jobName;
    }

    public ChangeStreamResultSet streamQuery(String partitionToken, Timestamp startTimestamp, Timestamp endTimestamp,
                                             long heartbeatMillis) {
        // For the initial partition we query with a null partition token
        final String partitionTokenOrNull =
            InitialPartition.isInitialPartition(partitionToken) ? null : partitionToken;
        String query;
        Statement statement;
        if (this.isPostgres()) {
            query = "SELECT * FROM \"spanner\".\"read_json_" + changeStreamName
                + "\"($1, $2, $3, $4, null)";
            statement = Statement.newBuilder(query)
                .bind("p1")
                .to(startTimestamp)
                .bind("p2")
                .to(endTimestamp)
                .bind("p3")
                .to(partitionTokenOrNull)
                .bind("p4")
                .to(heartbeatMillis)
                .build();
        } else {
            query = "SELECT * FROM READ_"
                + changeStreamName
                + "("
                + "   start_timestamp => @startTimestamp,"
                + "   end_timestamp => @endTimestamp,"
                + "   partition_token => @partitionToken,"
                + "   read_options => null,"
                + "   heartbeat_milliseconds => @heartbeatMillis"
                + ")";

            statement = Statement.newBuilder(query)
                .bind("startTimestamp")
                .to(startTimestamp)
                .bind("endTimestamp")
                .to(endTimestamp)
                .bind("partitionToken")
                .to(partitionTokenOrNull)
                .bind("heartbeatMillis")
                .to(heartbeatMillis)
                .build();
        }
        final ResultSet resultSet = databaseClient
            .singleUse()
            .executeQuery(statement, Options.priority(rpcPriority), Options.tag("job=" + jobName));

        return new ChangeStreamResultSet(resultSet);
    }

    public boolean isPostgres() {
        return this.databaseClient.getDialect() == Dialect.POSTGRESQL;
    }
}
