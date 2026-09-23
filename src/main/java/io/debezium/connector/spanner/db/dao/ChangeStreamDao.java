/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import java.util.List;
import java.util.Locale;

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
    private final boolean isMutableKeyRange;
    private final List<String> placementTvfNames;

    public ChangeStreamDao(String changeStreamName, boolean isMutableKeyRange, DatabaseClient databaseClient,
                           RpcPriority rpcPriority, String jobName) {
        this(changeStreamName, isMutableKeyRange, List.of(), databaseClient, rpcPriority, jobName);
    }

    public ChangeStreamDao(String changeStreamName, boolean isMutableKeyRange, List<String> placementTvfNames,
                           DatabaseClient databaseClient, RpcPriority rpcPriority, String jobName) {
        this.changeStreamName = changeStreamName;
        this.isMutableKeyRange = isMutableKeyRange;
        this.placementTvfNames = placementTvfNames;
        this.databaseClient = databaseClient;
        this.rpcPriority = rpcPriority;
        this.jobName = jobName;
    }

    public ChangeStreamResultSet streamQuery(String partitionToken, Timestamp startTimestamp, Timestamp endTimestamp,
                                             long heartbeatMillis) {
        return streamQuery(partitionToken, null, startTimestamp, endTimestamp, heartbeatMillis);
    }

    /**
     * Queries a single read table-valued function for change stream records.
     *
     * @param tvfName the placement-specific TVF to query (e.g. {@code READ_Foo_US}), or
     *     {@code null}/blank to fall back to the change stream's default {@code READ_<streamName>}
     *     function. Per {@code per_placement_tvf} change streams, callers are responsible for
     *     querying every placement TVF (see {@link #getPlacementTvfNames()}) and remembering which
     *     one produced a given partition, since each TVF has its own independent partition token
     *     space; this method does not union them itself.
     */
    public ChangeStreamResultSet streamQuery(String partitionToken, String tvfName, Timestamp startTimestamp, Timestamp endTimestamp,
                                             long heartbeatMillis) {
        // For the initial partition we query with a null partition token
        final String partitionTokenOrNull = InitialPartition.isInitialPartition(partitionToken) ? null : partitionToken;
        final String resolvedTvfName = (tvfName == null || tvfName.isBlank()) ? "READ_" + changeStreamName : tvfName;
        String query;
        Statement statement;
        if (this.isPostgres()) {
            String resolvedPostgresTvfName = resolvePostgresTvfName(tvfName);
            query = "SELECT * FROM \"spanner\".\"" + escapePostgresIdentifier(resolvedPostgresTvfName)
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
        }
        else {
            query = "SELECT * FROM "
                    + resolvedTvfName
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
                .executeQuery(statement, Options.priority(rpcPriority), Options.tag("kafka-spanner-connector-job=" + jobName));

        return new ChangeStreamResultSet(resultSet);
    }

    private String resolvePostgresTvfName(String tvfName) {
        if (tvfName == null || tvfName.isBlank()) {
            String prefix = isMutableKeyRange ? "read_proto_bytes_" : "read_json_";
            return prefix + changeStreamName.toLowerCase(Locale.ROOT);
        }

        return PostgresIdentifier.routineName(tvfName);
    }

    private String escapePostgresIdentifier(String identifier) {
        return identifier.replace("\"", "\"\"");
    }

    public boolean isPostgres() {
        return this.databaseClient.getDialect() == Dialect.POSTGRESQL;
    }

    public boolean isPerPlacementTvf() {
        return !placementTvfNames.isEmpty();
    }

    public String getChangeStreamName() {
        return changeStreamName;
    }

    public boolean isMutableKeyRange() {
        return isMutableKeyRange;
    }

    public List<String> getPlacementTvfNames() {
        return placementTvfNames;
    }
}
