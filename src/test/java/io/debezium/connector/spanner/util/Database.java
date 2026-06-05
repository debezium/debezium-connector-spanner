/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import java.util.UUID;

import com.google.cloud.spanner.Dialect;
import com.google.common.base.Strings;

import io.debezium.connector.spanner.config.BaseSpannerConnectorConfig;
import io.debezium.connector.spanner.db.DatabaseClientFactory;

public class Database {

    private static final String projectId = "test-project";
    private static final String instanceId = "test-instance";

    private final String databaseId;

    private Connection connection;

    private final Dialect dialect;

    private Database(String databaseId, Dialect dialect) {
        this.databaseId = databaseId;
        this.dialect = dialect;
    }

    public static String getSpannerOmniEndpoint() {
        return System.getProperty(BaseSpannerConnectorConfig.SPANNER_OMNI_ENDPOINT_PROPERTY_NAME);
    }

    public static boolean isSpannerOmniEndpoint() {
        return !Strings.isNullOrEmpty(getSpannerOmniEndpoint());
    }

    public static final Database TEST_DATABASE = Database.builder()
            .generateDatabaseId()
            .build();

    public static final Database TEST_PG_DATABASE = Database.builder()
            .generateDatabaseId()
            .dialect(Dialect.POSTGRESQL)
            .build();

    public String getProjectId() {
        return isSpannerOmniEndpoint() ? DatabaseClientFactory.SPANNER_OMNI_DEFAULT_ID : projectId;
    }

    public String getInstanceId() {
        return isSpannerOmniEndpoint() ? DatabaseClientFactory.SPANNER_OMNI_DEFAULT_ID : instanceId;
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public Connection getConnection() {
        if (this.connection != null) {
            return this.connection;
        }
        try {
            this.connection = new Connection(this).connect(dialect);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            Thread.currentThread().interrupt();
        }
        return this.connection;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String databaseId;

        private Dialect dialect = Dialect.GOOGLE_STANDARD_SQL;

        public Builder dialect(Dialect dialect) {
            this.dialect = dialect;
            return this;
        }

        public Builder databaseId(String databaseId) {
            this.databaseId = databaseId;
            return this;
        }

        public Builder generateDatabaseId() {
            String uuid = UUID.randomUUID().toString().replace("-", "")
                    .substring(0, 8);
            this.databaseId = "int_tests_" + uuid;
            return this;
        }

        public Database build() {
            return new Database(databaseId, dialect);
        }
    }
}
