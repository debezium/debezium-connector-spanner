/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.longrunning.OperationFuture;
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
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;

import io.debezium.connector.spanner.db.dao.SchemaDao;

public class Connection {

    private static final Logger LOG = LoggerFactory.getLogger(Connection.class);

    private final String projectId;
    private final String instanceId;
    private final String databaseId;
    private static final String emulatorHost = "http://localhost:9010";

    private DatabaseClient databaseClient;
    private Spanner spanner;
    private SchemaDao schemaDao;
    private final Dialect dialect;

    protected Connection(Database database) {
        this.projectId = database.getProjectId();
        this.instanceId = database.getInstanceId();
        this.databaseId = database.getDatabaseId();
        this.dialect = database.getDialect();
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
        OperationFuture<Void, UpdateDatabaseDdlMetadata> future = spanner.getDatabaseAdminClient().updateDatabaseDdl(instanceId, databaseId, updates, null);
        future.get();
    }

    public void createTable(String tableDefinition) throws ExecutionException, InterruptedException {
        this.updateDDL(List.of("create table " + tableDefinition));
    }

    public void createChangeStream(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables))));
        await().atMost(Duration.ofSeconds(60)).until(() -> isStreamExist(changeStreamName));
    }

    public void createChangeStreamNewValue(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            value_capture_type = 'NEW_VALUES'\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(60)).until(() -> isStreamExist(changeStreamName));
    }

    public void createChangeStreamNewRow(String changeStreamName, String... tables) throws ExecutionException,
            InterruptedException {
        this.updateDDL(List.of("create change stream " + changeStreamName + " for " +
                (tables.length == 0 ? "ALL" : String.join(",", tables)) +
                " OPTIONS (\n" +
                "            value_capture_type = 'NEW_ROW'\n" +
                "        ) "));
        await().atMost(Duration.ofSeconds(60)).until(() -> isStreamExist(changeStreamName));
    }

    private String createInstance() {
        for (Instance value : this.spanner.getInstanceAdminClient().listInstances().iterateAll()) {
            if (value.getId().getInstance().equals("test-instance")) {
                return "test-instance";
            }
        }
        String configId = "regional-us-central1";
        String displayName = "For IT";
        int nodeCount = 1;
        InstanceInfo instanceInfo = InstanceInfo.newBuilder(InstanceId.of(projectId, "test-instance"))
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
        return "test-instance";
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
            statement = Statement.newBuilder("select * from information_schema.tables where table_schema = '' and table_catalog = '' " +
                    "and table_name = $1")
                    .bind("p1").to(tableName).build();
        }
        else {
            statement = Statement.newBuilder("select * from information_schema.tables where table_schema = '' and table_catalog = '' " +
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
        createInstance();
        DatabaseAdminClient dbAdminClient = this.spanner.getDatabaseAdminClient();
        OperationFuture<com.google.cloud.spanner.Database, CreateDatabaseMetadata> operationFuture = dbAdminClient.createDatabase(
                dbAdminClient.newDatabaseBuilder(DatabaseId.of(projectId, instanceId, databaseId)).setDialect(dialect).build(),
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

    private void init() {
        SpannerOptions.Builder builder = SpannerOptions.newBuilder();

        builder.setCredentials(NoCredentials.getInstance());
        builder.setProjectId(projectId);
        builder.setEmulatorHost(emulatorHost);

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
