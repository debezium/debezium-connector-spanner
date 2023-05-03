/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.metadata;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.spanner.db.dao.SchemaDao;
import io.debezium.connector.spanner.db.model.schema.ChangeStreamSchema;
import io.debezium.connector.spanner.db.model.schema.Column;
import io.debezium.connector.spanner.db.model.schema.SpannerSchema;
import io.debezium.connector.spanner.db.model.schema.TableSchema;

/**
 * Stores schema of the Spanner change stream and database tables
 */
public class SchemaRegistry {

    private static final String DATABASE_SCHEMA_NOT_CACHED = "Database schema is not cached";

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistry.class);

    private final SchemaDao schemaDao;

    private volatile SpannerSchema spannerSchema;
    private volatile ChangeStreamSchema changeStream;
    private final String streamName;

    private volatile Timestamp timestamp;

    private final Runnable schemaResetTrigger;

    public SchemaRegistry(String streamName, SchemaDao schemaDao, Runnable schemaResetTrigger) {
        this.schemaDao = schemaDao;
        this.streamName = streamName;
        this.schemaResetTrigger = schemaResetTrigger;
    }

    public void init() {
        forceUpdateSchema(null, Timestamp.now(), null);
    }

    public synchronized TableSchema getWatchedTable(TableId tableId) {
        if (this.timestamp == null) {
            throw new IllegalStateException(DATABASE_SCHEMA_NOT_CACHED);
        }
        TableSchema table = spannerSchema.getTable(tableId);

        ChangeStreamSchema.Table streamTable = changeStream.getTable(table.getName());

        List<Column> columns = table.columns().stream()
                .filter(column -> column.isPrimaryKey() || streamTable.hasColumn(column.getName()))
                .collect(Collectors.toList());

        return new TableSchema(table.getName(), columns);
    }

    public synchronized void checkSchema(TableId tableId, Timestamp timestamp, List<Column> rowType) {
        if (!validate(tableId, rowType)) {
            if (this.updateSchema(tableId, timestamp, rowType)) {
                schemaResetTrigger.run();
                return;
            }
            LOGGER.warn("Schema has not been updated");
        }
    }

    @VisibleForTesting
    boolean validate(TableId tableId, List<Column> rowType) {
        if (this.timestamp == null) {
            throw new IllegalStateException(DATABASE_SCHEMA_NOT_CACHED);
        }
        TableSchema schemaTable = this.spannerSchema.getTable(tableId);
        TableSchema watchedTable = this.getWatchedTable(tableId);

        if (schemaTable == null) {
            LOGGER.warn("Table not found in registry : {}", tableId.getTableName());
            return false;
        }

        return SchemaValidator.validate(schemaTable, watchedTable, rowType);
    }

    public synchronized boolean updateSchema(TableId tableId, Timestamp updatedTimestamp, List<Column> rowType) {
        if (updatedTimestamp.equals(this.timestamp)) {
            return false;
        }

        LOGGER.info("Schema is outdated. Try to update schema registry...");
        forceUpdateSchema(tableId, updatedTimestamp, rowType);
        LOGGER.info("Schema registry has been updated to date {}", updatedTimestamp);
        return true;
    }

    public void updateSchemaFromStaleTimestamp(TableId tableId, Timestamp timestamp, List<Column> rowType) {
        LOGGER.info("Schema is outdated. Try to update schema registry outside of retention period...");
        SpannerSchema.SpannerSchemaBuilder builder = SpannerSchema.builder();
        Dialect dialect = this.schemaDao.isPostgres() ? Dialect.POSTGRESQL : Dialect.GOOGLE_STANDARD_SQL;
        for (Column column : rowType) {
            if (column.isPrimaryKey()) {
                builder.addPrimaryColumn(tableId.getTableName(), column.getName());
            }
            builder.addColumn(tableId.getTableName(), column.getName(), column.getType().getType().toString(), column.getOrdinalPosition(),
                    column.isNullable(), dialect);

        }
        SpannerSchema newSchema = builder.build();
        if (this.spannerSchema == null) {
            this.spannerSchema = newSchema;
            return;
        }
        this.spannerSchema = SchemaMerger.merge(this.spannerSchema, newSchema);
        LOGGER.info("Schema registry has been updated to stale timestamp {}", timestamp);

    }

    @VisibleForTesting
    void forceUpdateSchema(@Nullable TableId tableId, Timestamp updatedTimestamp, @Nullable List<Column> rowTypes) {
        try {
            this.timestamp = updatedTimestamp;
            this.changeStream = schemaDao.getStream(timestamp, streamName);

            if (this.changeStream == null) {
                throw new IllegalStateException("Change stream doesn't exist at timestamp: "
                        + streamName + ", " + timestamp);
            }

            SpannerSchema newSchema;
            if (this.changeStream.isWatchedAllTables()) {
                newSchema = schemaDao.getSchema(timestamp);
            }
            else {
                newSchema = schemaDao.getSchema(timestamp, this.changeStream.getTables());
            }
            if (this.spannerSchema == null) {
                this.spannerSchema = newSchema;
                return;
            }
            this.spannerSchema = SchemaMerger.merge(this.spannerSchema, newSchema);
        }
        catch (SpannerException e) {
            if (e.getMessage().contains("has exceeded the maximum timestamp staleness") && e.getErrorCode().equals(ErrorCode.FAILED_PRECONDITION)) {
                updateSchemaFromStaleTimestamp(tableId, timestamp, rowTypes);
            }
        }
    }

    public Set<TableId> getAllTables() {
        if (this.spannerSchema == null) {
            throw new IllegalStateException("database schema is not cached yet");
        }
        return this.spannerSchema.getAllTables();
    }
}
