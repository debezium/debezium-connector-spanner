/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;

/**
 * Lazily provides a shared {@link Connection} per {@link Database} (dialect) to a real Cloud
 * Spanner instance for tests annotated with {@link RealSpannerCompatible}. Connections are only
 * created when a real-Spanner-compatible test class requests them, so the rest of the suite
 * running against the emulator is unaffected. Cached per {@link Database} rather than as a single
 * global connection, so GoogleSQL- and PostgreSQL-dialect tests running in the same JVM (e.g.
 * {@link MutableKeyRangeIT}, parameterized over dialect) each get their own real connection.
 */
public final class RealSpannerTestSupport {

    private static final Map<Database, Connection> connections = new ConcurrentHashMap<>();

    private RealSpannerTestSupport() {
    }

    /**
     * Returns a shared {@link Connection} explicitly targeting real Spanner for the given
     * {@link Database}. The caller must have set {@code -Dspanner.test.real=true} and supplied the
     * usual credentials/endpoint properties.
     */
    public static Connection getConnection(Database database) {
        if (!Connection.isRealSpanner()) {
            throw new IllegalStateException(
                    "RealSpannerTestSupport.getConnection() requires -Dspanner.test.real=true");
        }
        return connections.computeIfAbsent(database, Database::getRealConnection);
    }
}
