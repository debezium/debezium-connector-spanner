/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;

/**
 * Lazily provides a single {@link Connection} to a real Cloud Spanner instance for tests annotated
 * with {@link RealSpannerCompatible}. The connection is only created when a real-Spanner-compatible
 * test class requests it, so the rest of the suite running against the emulator is unaffected.
 */
public final class RealSpannerTestSupport {

    private static volatile Connection connection;

    private RealSpannerTestSupport() {
    }

    /**
     * Returns a shared {@link Connection} explicitly targeting real Spanner. The caller must have
     * set {@code -Dspanner.test.real=true} and supplied the usual credentials/endpoint properties.
     */
    public static Connection getConnection(Database database) {
        if (!Connection.isRealSpanner()) {
            throw new IllegalStateException(
                    "RealSpannerTestSupport.getConnection() requires -Dspanner.test.real=true");
        }
        if (connection == null) {
            synchronized (RealSpannerTestSupport.class) {
                if (connection == null) {
                    connection = database.getRealConnection();
                }
            }
        }
        return connection;
    }
}
