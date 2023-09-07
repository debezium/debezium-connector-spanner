/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

// import io.debezium.embedded.EmbeddedEngine;

public class SpannerConnectorIT extends AbstractSpannerConnectorIT {

    private SpannerConnector connector;

    @Test
    public void testDocker() {
        System.out.println("test hahahaha");
        assertEquals(10, 9 + 1);
        // try {
        // Thread.sleep(10000);
        // }
        // catch (InterruptedException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }

    }
}
