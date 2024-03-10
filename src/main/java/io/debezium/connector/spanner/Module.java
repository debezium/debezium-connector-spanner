/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import io.debezium.util.VersionParser;

public final class Module {

    private Module() {
    }

    public static String version() {
        return VersionParser.getVersion();
    }

    /**
     * @return symbolic name of the connector plugin
     */
    public static String name() {
        return "spanner";
    }

    /**
     * @return context name used in log MDC and JMX metrics
     */
    public static String contextName() {
        return "Spanner";
    }
}
