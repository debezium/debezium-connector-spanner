/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;

import io.debezium.connector.spanner.db.model.InitialPartition;

/**
 * Executes streaming queries to the Spanner database
 */
public class ChangeStreamDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamDao.class);
    private static final long DEFAULT_PROBE_HEARTBEAT_MILLIS = 10_000L;
    private static final int MAX_PROBE_CACHE_SIZE = 10_000;
    private static final String PLACEMENT_MISMATCH_ERROR_SUBSTRING = "does not belong to the placement TVF";

    private final String changeStreamName;
    private final DatabaseClient databaseClient;
    private final RpcPriority rpcPriority;
    private final String jobName;
    private final boolean isMutableKeyRange;
    private final List<String> placementTvfNames;
    private final ConcurrentMap<String, Boolean> externalPlacementTokenCache = new ConcurrentHashMap<>();

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

    /**
     * Probes the configured placement TVFs to determine whether {@code partitionToken} belongs
     * to an external placement not tracked by this connector instance.
     *
     * <p>Returns {@code true} only if {@code per_placement_tvf} is enabled and every configured
     * placement TVF rejects {@code partitionToken} with {@code INVALID_ARGUMENT: Partition token ...
     * does not belong to the placement TVF}. Results are cached per token so each unknown source
     * token is probed at most once. Transient Spanner errors return {@code false} without caching
     * so that the caller keeps the gate closed and retries on the next check.
     */
    public boolean isExternalPlacementToken(String partitionToken, Timestamp probeTimestamp) {
        if (!isPerPlacementTvf()
                || partitionToken == null
                || InitialPartition.isInitialPartition(partitionToken)
                || probeTimestamp == null) {
            return false;
        }
        Boolean cached = externalPlacementTokenCache.get(partitionToken);
        if (cached != null) {
            return cached;
        }
        for (String tvfName : placementTvfNames) {
            try (ChangeStreamResultSet resultSet = streamQuery(
                    partitionToken, tvfName, probeTimestamp, probeTimestamp, DEFAULT_PROBE_HEARTBEAT_MILLIS)) {
                resultSet.next();
                cacheProbeResult(partitionToken, false);
                return false;
            }
            catch (SpannerException e) {
                if (isPlacementMismatchException(e)) {
                    LOGGER.debug("Partition token {} rejected by placement TVF {} with placement mismatch",
                            partitionToken, tvfName);
                    continue;
                }
                LOGGER.warn("Unexpected Spanner error while probing partition token {} against TVF {}; "
                        + "keeping MoveIn gate closed for retry",
                        partitionToken, tvfName, e);
                return false;
            }
        }
        LOGGER.info("Partition token {} rejected by all configured placement TVFs {}; treating as external placement token",
                partitionToken, placementTvfNames);
        cacheProbeResult(partitionToken, true);
        return true;
    }

    private void cacheProbeResult(String partitionToken, boolean isExternal) {
        if (externalPlacementTokenCache.size() >= MAX_PROBE_CACHE_SIZE) {
            externalPlacementTokenCache.clear();
        }
        externalPlacementTokenCache.put(partitionToken, isExternal);
    }

    private static boolean isPlacementMismatchException(SpannerException e) {
        return e.getErrorCode() == ErrorCode.INVALID_ARGUMENT
                && e.getMessage() != null
                && e.getMessage().contains(PLACEMENT_MISMATCH_ERROR_SUBSTRING);
    }
}
