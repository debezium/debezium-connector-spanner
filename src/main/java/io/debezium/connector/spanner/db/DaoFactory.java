/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db;

import java.util.Collections;
import java.util.List;

import com.google.cloud.spanner.Options;

import io.debezium.connector.spanner.db.dao.ChangeStreamDao;
import io.debezium.connector.spanner.db.dao.SchemaDao;
import io.debezium.connector.spanner.db.model.ChangeStreamOptions;

/**
 * Factory for {@code ChangeStreamDao}
 */
public class DaoFactory {
    private final DatabaseClientFactory databaseClientFactory;

    private SchemaDao schemaDao;

    public DaoFactory(DatabaseClientFactory databaseClientFactory) {
        this.databaseClientFactory = databaseClientFactory;
    }

    public SchemaDao getSchemaDao() {
        if (schemaDao != null) {
            return schemaDao;
        }
        this.schemaDao = new SchemaDao(this.databaseClientFactory.getDatabaseClient());
        return schemaDao;
    }

    public ChangeStreamDao getStreamDao(String changeStreamName,
                                        Options.RpcPriority rpcPriority, String jobName) {
        return getStreamDao(changeStreamName, Collections.emptyList(), rpcPriority, jobName);
    }

    public ChangeStreamDao getStreamDao(String changeStreamName, List<String> placementTvfNames,
                                        Options.RpcPriority rpcPriority, String jobName) {
        SchemaDao schemaDao = getSchemaDao();
        ChangeStreamOptions streamOptions = schemaDao.getChangeStreamOptions(changeStreamName);
        boolean isMutableKeyRange = streamOptions.isMutableKeyRange();
        if (!placementTvfNames.isEmpty() && !isMutableKeyRange) {
            throw new IllegalArgumentException("gcp.spanner.placement.tvf.names is only supported for change streams "
                    + "with MUTABLE_KEY_RANGE partition mode; change stream '" + changeStreamName + "' is not one.");
        }
        schemaDao.validatePlacementTvfNames(changeStreamName, placementTvfNames, streamOptions);
        return new ChangeStreamDao(changeStreamName, isMutableKeyRange, placementTvfNames,
                this.databaseClientFactory.getDatabaseClient(), rpcPriority, jobName);
    }
}
