/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.CHANGE_STREAM_NAME;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.DATABASE_ID;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.DATABASE_ROLE;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.INSTANCE_ID;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.PROJECT_ID;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SPANNER_CREDENTIALS_JSON;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SPANNER_CREDENTIALS_PATH;
import static io.debezium.connector.spanner.config.BaseSpannerConnectorConfig.SPANNER_HOST;
import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.spanner.db.DatabaseClientFactory;

/**
 * Used to validate the Spanner Change Stream provided in configuration
 */
public class ChangeStreamValidator implements ConfigurationValidator.Validator {
    private static final Logger LOGGER = getLogger(ChangeStreamValidator.class);
    private final ConfigurationValidator.ValidationContext context;

    private boolean result;

    private ChangeStreamValidator(ConfigurationValidator.ValidationContext context) {
        this.context = context;
    }

    /**
     * Create ChangeStreamValidator with validationContext
     * @param validationContext
     * @return ChangeStreamValidator
     */
    public static ChangeStreamValidator withContext(ConfigurationValidator.ValidationContext validationContext) {
        return new ChangeStreamValidator(validationContext);
    }

    /**
     * Returns true if validation passed
     * @return
     */
    @Override
    public boolean isSuccess() {
        return result;
    }

    /**
     * Does the validation
     * @return this object
     */
    public ChangeStreamValidator validate() {

        String changeStreamName = context.getString(CHANGE_STREAM_NAME);

        DatabaseClientFactory databaseClientFactory = new DatabaseClientFactory(context.getString(PROJECT_ID),
                context.getString(INSTANCE_ID),
                context.getString(DATABASE_ID),
                context.getString(SPANNER_CREDENTIALS_JSON),
                context.getString(SPANNER_CREDENTIALS_PATH),
                context.getString(SPANNER_HOST),
                context.getString(DATABASE_ROLE));

        this.result = isStreamExist(databaseClientFactory.getDatabaseClient(), changeStreamName);

        if (!result) {
            String msg = "ChangeStream '" + changeStreamName + "' doesn't exist";
            LOGGER.error(msg);
            context.error(msg, CHANGE_STREAM_NAME);
        }
        return this;
    }

    /**
     * Checks change stream is exist
     * @param databaseClient DatabaseClient object
     * @param streamName name of stream
     * @return true if change stream exist
     */
    @VisibleForTesting
    boolean isStreamExist(DatabaseClient databaseClient, String streamName) {
        Statement statement;
        if (databaseClient.getDialect() == Dialect.POSTGRESQL) {
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
                    .to(streamName)
                    .build();
        }
        return databaseClient.singleUse().executeQuery(statement).next();
    }
}
