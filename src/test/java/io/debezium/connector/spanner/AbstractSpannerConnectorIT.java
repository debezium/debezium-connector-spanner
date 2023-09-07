/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.connector.spanner.util.EmulatorEnvironment;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

public class AbstractSpannerConnectorIT extends AbstractConnectorTest {
    private static final EmulatorEnvironment EMULATOR_ENVIRONMENT = new EmulatorEnvironment();
    // static {
    // Testing.Print.enable();
    // if (!EMULATOR_ENVIRONMENT.isStarted()) {
    // EMULATOR_ENVIRONMENT.startLocalEmulator();
    // }
    // }
    protected static final Database database = Database.TEST_DATABASE;
    protected static Connection databaseConnection;
    protected static final int WAIT_FOR_CDC = 3 * 1000;

    @AfterAll
    public static void after() throws InterruptedException {
        System.out.println("after all");
        EMULATOR_ENVIRONMENT.shutDownLocalEmulator();
    }

    @BeforeAll
    public static void before() throws InterruptedException {
        Testing.Print.enable();
        System.out.println("before all");
        if (!EMULATOR_ENVIRONMENT.isStarted()) {
            EMULATOR_ENVIRONMENT.startLocalEmulator();
        }
        databaseConnection = database.getConnection();
    }

    protected static void waitForCDC() {
        try {
            Thread.sleep(WAIT_FOR_CDC);
        }
        catch (Exception e) {

        }
    }

    protected static void waitForCDC(long minutes) {
        try {
            Thread.sleep(Duration.ofMinutes(minutes).toMillis());
        }
        catch (Exception e) {
        }
    }
}
