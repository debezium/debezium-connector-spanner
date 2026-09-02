/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

public class BasicSanityCheckIT extends AbstractSpannerConnectorIT {

    private static final String tableName = "embedded_sanity_tests_table";
    private static final String changeStreamName = "embeddedSanityTestChangeStream";

    @Test
    public void shouldNotStartConnectorWithoutRequireConfigs() throws InterruptedException {
        // Config with only instance id provided.
        Configuration config = Configuration.create()
                .with("gcp.spanner.instance.id", database.getInstanceId())
                .build();
        start(SpannerConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(msg.contains("Connector configuration is not valid"));
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldNotStartConnectorWithoutNonExistentChangeStreams() throws InterruptedException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", "fooBar")
                .with("name", tableName + "_test")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();
        start(SpannerConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(msg.contains("ChangeStream 'fooBar' doesn't exist or you don't have sufficient permissions"));
        });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldNotStartConnectorWithOutOfRangeHeartbeatMillis() throws InterruptedException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("heartbeat.interval.ms", "1")
                .with("gcp.spanner.start.time",
                        DateTimeFormatter.ISO_INSTANT.format(Instant.now().plus(2, ChronoUnit.DAYS)))
                .build();
        start(SpannerConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(msg.contains("Heartbeat interval must be between 100 and 300000"));
        });
        assertConnectorNotRunning();
    }
}
