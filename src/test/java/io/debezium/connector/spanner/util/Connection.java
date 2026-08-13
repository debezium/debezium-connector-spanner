/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import static io.debezium.connector.spanner.util.Database.isSpannerOmniEndpoint;
import static org.awaitility.Awaitility.await;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.Strings;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.ListValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.database.v1.DatabaseName;
import com.google.spanner.admin.database.v1.SplitPoints;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;

import io.debezium.connector.spanner.db.DatabaseClientFactory;
import io.debezium.connector.spanner.db.dao.SchemaDao;
import io.grpc.ManagedChannelBuilder;

public class Connection {

    private static final Logger LOG = LoggerFactory.getLogger(Connection.class);

    private final String projectId;
    private final String instanceId;
    private final String databaseId;
    public static final String emulatorHost = "http://localhost:9010";

    private static final String REAL_SPANNER_PROPERTY = "spanner.test.real";
    private static final String CREDENTIALS_PATH_PROPERTY = "gcp.spanner.credentials.path";
    private static final String CREDENTIALS_JSON_PROPERTY = "gcp.spanner.credentials.json";
    private static final String HOST_PROPERTY = "gcp.spanner.host";

    private static long ddlWaitTimeSeconds() {
        return Long.parseLong(System.getProperty("debezium.test.spanner.ddl.waittime", "60"));
    }

    public DatabaseClient databaseClient;
    private Spanner spanner;
    private SchemaDao schemaDao;
    private final Dialect dialect;
    private final boolean realSpanner;

    protected Connection(Database database) {
        this(database, false);
    }

    protected Connection(Database database, boolean realSpanner) {
        this.projectId = database.getProjectId();
        this.instanceId = database.getInstanceId();
        this.databaseId = database.getDatabaseId();
        this.dialect = database.getDialect();
        this.realSpanner = realSpanner;
    }

    public ResultSet executeSelect(String query) {
        return databaseClient.singleUse().executeQuery(Statement.of(query));
    }

    public ResultSet executeSelect(Statement statement) {
        return databaseClient.singleUse().executeQuery(statement);
    }

    public Long executeUpdate(String query) {
        final String msg = "Execution result: {}, query: {}";
        return databaseClient.readWriteTransaction()
                .run(transaction -> {
                    final var uuid = UUID.randomUUID().toString();
                    LOG.info("Begin transaction {}", uuid);
                    final var res = transaction.executeUpdate(Statement.of(query));
                    if (res > 0L) {
                        LOG.info(msg, res, query);
                    }
                    else {
                        LOG.warn(msg, res, query);
                    }
                    return res;
                });
    }

    public Long executeUpdate(List<String> queries) {
        final String msg = "Execution result: {}, query: {}";
        return databaseClient.readWriteTransaction()
                .run(transaction -> {
                    final var uuid = UUID.randomUUID().toString();
                    LOG.info("Begin transaction {}", uuid);
                    var result = 0L;
                    for (final var query : queries) {
                        final var res = transaction.executeUpdate(Statement.of(query));
                        result += res;
                        if (res > 0L) {
                            LOG.info(msg, res, query);
                        }
                        else {
                            LOG.warn(msg, res, query);
                        }
                    }
                    LOG.info("End transaction {}, result : {}", uuid, result);
                    return result;
                });
    }

    public void updateDDL(Iterable<String> updates) throws ExecutionException, InterruptedException {
        OperationFuture<Void, UpdateDatabaseDdlMetadata> future = spanner.getDatabaseAdminClient()
                .updateDatabaseDdl(instanceId, databaseId, updates, null);
        future.get();
    }

    public void createTable(String tableDefinition) throws ExecutionException, InterruptedException {
        this.updateDDL(List.of("create table " + tableDefinition));
    }

    public void createChangeStream(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables))));
        await().atMost(Duration.ofSeconds(ddlWaitTimeSeconds())).until(() -> isStreamExist(changeStreamName));
    }

    public void createMutableKeyRangeChangeStream(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (partition_mode = 'MUTABLE_KEY_RANGE')"));
        await().atMost(Duration.ofSeconds(ddlWaitTimeSeconds())).until(() -> isStreamExist(changeStreamName));
    }

    private static final Duration DEFAULT_SPLIT_EXPIRY = Duration.ofMinutes(30);

    public void forceSplit(String tableName, String... keyParts) {
        forceSplit(tableName, DEFAULT_SPLIT_EXPIRY, keyParts);
    }

    /**
     * Forces Spanner to split the key range of {@code tableName} at the given key value(s),
     * triggering a mutable key range move (MoveOut/MoveIn) for change streams tracking the table.
     * Uses the {@code AddSplitPoints} admin API, which requires the
     * {@code spanner.databases.addSplitPoints} permission (granted by the
     * {@code roles/spanner.databaseAdmin} IAM role).
     *
     * <p>Cloud Spanner enforces a strict per-minute quota on the number of split points that can
     * be processed (as low as 1/minute on small test instances), so calls are retried with a
     * cooldown when the request is throttled with {@code RESOURCE_EXHAUSTED}.
     */
    public void forceSplit(String tableName, Duration expiry, String... keyParts) {
        int maxAttempts = 4;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try (com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient adminClient = spanner.createDatabaseAdminClient()) {
                Instant expiryInstant = Instant.now().plus(expiry);
                Timestamp expireTime = Timestamp.newBuilder()
                        .setSeconds(expiryInstant.getEpochSecond())
                        .setNanos(expiryInstant.getNano())
                        .build();

                ListValue.Builder keyValues = ListValue.newBuilder();
                for (String keyPart : keyParts) {
                    keyValues.addValues(Value.newBuilder().setStringValue(keyPart).build());
                }

                SplitPoints splitPoint = SplitPoints.newBuilder()
                        .setTable(tableName)
                        .setExpireTime(expireTime)
                        .addKeys(SplitPoints.Key.newBuilder().setKeyParts(keyValues))
                        .build();

                adminClient.addSplitPoints(DatabaseName.of(projectId, instanceId, databaseId), List.of(splitPoint));
                LOG.info("Forced split on table {} at key {}", tableName, List.of(keyParts));
                return;
            }
            catch (ResourceExhaustedException e) {
                if (attempt == maxAttempts) {
                    throw new RuntimeException("Failed to force split for table " + tableName
                            + " after " + maxAttempts + " attempts (split point quota exhausted)", e);
                }
                LOG.warn("Split point quota exhausted for table {}, retrying in 65s (attempt {}/{})", tableName, attempt, maxAttempts);
                try {
                    Thread.sleep(65_000);
                }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting to retry split for table " + tableName, interrupted);
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to force split for table " + tableName, e);
            }
        }
    }

    public void createChangeStreamNewValue(String changeStreamName, PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        createChangeStreamWithValueCaptureType(changeStreamName, "NEW_VALUES", partitionMode, tables);
    }

    public void createChangeStreamNewRow(String changeStreamName, PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        createChangeStreamWithValueCaptureType(changeStreamName, "NEW_ROW", partitionMode, tables);
    }

    public void createChangeStreamNewRowAndOldValues(String changeStreamName, PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        createChangeStreamWithValueCaptureType(changeStreamName, "NEW_ROW_AND_OLD_VALUES", partitionMode, tables);
    }

    private void createChangeStreamWithValueCaptureType(String changeStreamName, String valueCaptureType,
                                                        PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            value_capture_type = '" + valueCaptureType + "',\n" +
                "            partition_mode = '" + partitionMode.name() + "'\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(ddlWaitTimeSeconds())).until(() -> isStreamExist(changeStreamName));
    }

    public void createChangeStreamExcludeDelete(String changeStreamName, PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        createChangeStreamWithBooleanOption(changeStreamName, "exclude_delete", partitionMode, tables);
    }

    public void createChangeStreamExcludeInsert(String changeStreamName, PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        createChangeStreamWithBooleanOption(changeStreamName, "exclude_insert", partitionMode, tables);
    }

    public void createChangeStreamExcludeUpdate(String changeStreamName, PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        createChangeStreamWithBooleanOption(changeStreamName, "exclude_update", partitionMode, tables);
    }

    public void createChangeStreamAllowTxnExclusion(String changeStreamName, PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        createChangeStreamWithBooleanOption(changeStreamName, "allow_txn_exclusion", partitionMode, tables);
    }

    private void createChangeStreamWithBooleanOption(String changeStreamName, String optionName,
                                                     PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            " + optionName + " = true,\n" +
                "            partition_mode = '" + partitionMode.name() + "'\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(ddlWaitTimeSeconds())).until(() -> isStreamExist(changeStreamName));
    }

    public void createChangeStreamExcludeTtlDeletes(String changeStreamName, PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        createChangeStreamWithBooleanOption(changeStreamName, "exclude_ttl_deletes", partitionMode, tables);
    }

    public void createChangeStream(String changeStreamName, PartitionMode partitionMode, String... tables)
            throws ExecutionException, InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS ( partition_mode = '" + partitionMode.name() + "' )"));
        await().atMost(Duration.ofSeconds(60)).until(() -> isStreamExist(changeStreamName));
    }

    public void createPlacement(String placementName, String instancePartitionId) throws ExecutionException, InterruptedException {
        if (!instancePartitionExists(instancePartitionId)) {
            throw new IllegalStateException(
                    "Instance partition '" + instancePartitionId + "' does not exist on instance '" + instanceId
                            + "'. Provision it first - see doc/real-spanner-testing.md, e.g.:\n"
                            + "  gcloud spanner instance-partitions create " + instancePartitionId
                            + " --instance=" + instanceId + " --project=" + projectId + " --config=<config> --nodes=1");
        }
        this.updateDDL(List.of("create placement " + placementName +
                " OPTIONS ( instance_partition = '" + instancePartitionId + "' )"));
        await().atMost(Duration.ofSeconds(ddlWaitTimeSeconds())).until(() -> placementExists(placementName));
    }

    private boolean instancePartitionExists(String instancePartitionId) {
        String name = String.format("projects/%s/instances/%s/instancePartitions/%s", projectId, instanceId, instancePartitionId);
        try (com.google.cloud.spanner.admin.instance.v1.InstanceAdminClient adminClient = spanner.createInstanceAdminClient()) {
            adminClient.getInstancePartition(name);
            return true;
        }

        catch (com.google.api.gax.rpc.NotFoundException e) {
            return false;
        }
    }

    public boolean dropPlacement(String placementName) throws InterruptedException {
        try {
            if (!placementExists(placementName)) {
                return false;
            }
            this.updateDDL(List.of("drop placement " + placementName));
        }
        catch (ExecutionException ex) {
            LOG.warn("Can`t drop placement", ex);
            return false;
        }
        return true;
    }

    private boolean placementExists(String placementName) {
        Statement statement = Statement.newBuilder("select placement_name " +
                "from information_schema.placements " +
                "where placement_name = @placementName")
                .bind("placementName").to(placementName).build();
        try (ResultSet resultSet = this.executeSelect(statement)) {
            return resultSet.next();
        }
    }

    private String createInstance() {
        if (isSpannerOmniEndpoint()) {
            return DatabaseClientFactory.SPANNER_OMNI_DEFAULT_ID;
        }
        for (Instance value : this.spanner.getInstanceAdminClient().listInstances().iterateAll()) {
            if (value.getId().getInstance().equals(instanceId)) {
                return instanceId;
            }
        }
        String configId = "regional-us-central1";
        String displayName = "For IT";
        int nodeCount = 1;
        InstanceInfo instanceInfo = InstanceInfo.newBuilder(InstanceId.of(projectId, instanceId))
                .setInstanceConfigId(InstanceConfigId.of(projectId, configId))
                .setNodeCount(nodeCount)
                .setDisplayName(displayName)
                .build();

        OperationFuture<Instance, CreateInstanceMetadata> instance = this.spanner.getInstanceAdminClient()
                .createInstance(instanceInfo);
        try {
            instance.get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return instanceId;
    }

    private boolean isStreamExist(String streamName) {
        Statement statement;
        if (schemaDao.isPostgres()) {
            statement = Statement.newBuilder("select change_stream_name " +
                    "from information_schema.change_streams cs " +
                    "where cs.change_stream_name = $1")
                    .bind("p1")
                    .to(streamName.toLowerCase())
                    .build();
        }
        else {
            statement = Statement.newBuilder("select change_stream_name " +
                    "from information_schema.change_streams cs " +
                    "where cs.change_stream_name = @streamname")
                    .bind("streamName")
                    .to(streamName).build();
        }
        return databaseClient.singleUse().executeQuery(statement).next();
    }

    public boolean dropTable(String tableName) throws InterruptedException {
        try {
            if (!isTableExist(tableName)) {
                return false;
            }
            this.updateDDL(List.of("drop table " + tableName));
        }
        catch (ExecutionException ex) {
            LOG.warn("Can`t drop table", ex);
            return false;
        }
        return true;
    }

    public boolean dropChangeStream(String changeStreamName) throws InterruptedException {
        try {
            if (!this.isChangeStreamExist(changeStreamName)) {
                return false;
            }
            this.updateDDL(List.of("drop change stream " + changeStreamName));

        }
        catch (ExecutionException ex) {
            LOG.warn("Can`t delete change stream", ex);
            return false;
        }
        return true;
    }

    public boolean isChangeStreamExist(String changeStreamName) {
        Statement statement;
        if (schemaDao.isPostgres()) {
            statement = Statement.newBuilder("select * from information_schema.change_streams " +
                    "where change_stream_name = $1")
                    .bind("p1").to(changeStreamName).build();
        }
        else {
            statement = Statement.newBuilder("select * from information_schema.change_streams " +
                    "where change_stream_name = @streamName")
                    .bind("streamName").to(changeStreamName).build();
        }
        try (ResultSet resultSet = this.executeSelect(statement)) {
            return resultSet.next();
        }
    }

    public boolean isTableExist(String tableName) {
        Statement statement;
        if (schemaDao.isPostgres()) {
            statement = Statement
                    .newBuilder(
                            "select * from information_schema.tables where table_schema = '' and table_catalog = '' " +
                                    "and table_name = $1")
                    .bind("p1").to(tableName).build();
        }
        else {
            statement = Statement
                    .newBuilder(
                            "select * from information_schema.tables where table_schema = '' and table_catalog = '' " +
                                    "and table_name = @tableName")
                    .bind("tableName").to(tableName).build();
        }
        try (ResultSet resultSet = this.executeSelect(statement)) {
            return resultSet.next();
        }
    }

    public boolean isDatabaseExist(String databaseId) {
        try {
            return this.spanner.getDatabaseAdminClient().getDatabase(instanceId, databaseId) != null;
        }
        catch (Exception ex) {
            return false;
        }
    }

    public void dropDatabase(String databaseId) {
        this.spanner.getDatabaseAdminClient().dropDatabase(instanceId, databaseId);
        LOG.info("{} database has been dropped", databaseId);
    }

    public void createDatabase(String databaseId, Dialect dialect) throws InterruptedException {
        if (!isSpannerOmniEndpoint() && !realSpanner) {
            createInstance();
        }
        DatabaseAdminClient dbAdminClient = this.spanner.getDatabaseAdminClient();
        OperationFuture<com.google.cloud.spanner.Database, CreateDatabaseMetadata> operationFuture = dbAdminClient
                .createDatabase(
                        dbAdminClient.newDatabaseBuilder(DatabaseId.of(projectId, instanceId, databaseId))
                                .setDialect(dialect).build(),
                        Collections.emptyList());
        try {
            operationFuture.get();
        }
        catch (ExecutionException ex) {
            throw new RuntimeException("Failed to create database", ex);
        }
        LOG.info("{} database has been created", databaseId);
    }

    public Connection connect(Dialect dialect) throws InterruptedException {
        if (this.databaseClient != null) {
            return this;
        }

        this.init();

        if (isDatabaseExist(databaseId)) {
            this.dropDatabase(databaseId);
        }

        this.createDatabase(databaseId, dialect);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> this.dropDatabase(databaseId)));

        this.databaseClient = this.spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
        this.schemaDao = new SchemaDao(databaseClient);

        return this;
    }

    public static boolean isRealSpanner() {
        return Boolean.parseBoolean(System.getProperty(REAL_SPANNER_PROPERTY, "false"));
    }

    private GoogleCredentials getCredentials() {
        String credentialsPath = System.getProperty(CREDENTIALS_PATH_PROPERTY);
        String credentialsJson = System.getProperty(CREDENTIALS_JSON_PROPERTY);
        try {
            if (!Strings.isNullOrEmpty(credentialsPath)) {
                return GoogleCredentials.fromStream(new FileInputStream(credentialsPath));
            }
            if (!Strings.isNullOrEmpty(credentialsJson)) {
                return GoogleCredentials.fromStream(new ByteArrayInputStream(credentialsJson.getBytes()));
            }
            return GoogleCredentials.getApplicationDefault();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to load Google credentials for real Spanner tests", e);
        }
    }

    private void init() {
        SpannerOptions.Builder builder = SpannerOptions.newBuilder();

        builder.setProjectId(projectId);
        if (isSpannerOmniEndpoint()) {
            builder.setExperimentalHost(System.getProperty(HOST_PROPERTY));
            if (Boolean.parseBoolean(System.getProperty("spanner.omni.use.plaintext", "false"))) {
                builder.setChannelConfigurator(ManagedChannelBuilder::usePlaintext);
            }
            else if (!Strings.isNullOrEmpty(System.getProperty("spanner.omni.client.key.path"))
                    && !Strings.isNullOrEmpty(System.getProperty("spanner.omni.client.cert.path"))) {
                builder.useClientCert(System.getProperty("spanner.omni.client.cert.path"), System.getProperty("spanner.omni.client.key.path"));
            }
            builder.setBuiltInMetricsEnabled(false)
                    .setCredentials(NoCredentials.getInstance());
        }
        else if (realSpanner) {
            builder.setCredentials(getCredentials());
            String host = System.getProperty(HOST_PROPERTY);
            if (!Strings.isNullOrEmpty(host)) {
                builder.setHost(host);
            }
        }
        else {
            builder.setCredentials(NoCredentials.getInstance());
            builder.setEmulatorHost(emulatorHost);
        }

        SpannerOptions options = builder.build();
        try {
            this.spanner = options.getService();
        }
        catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
