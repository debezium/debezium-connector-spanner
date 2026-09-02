/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Dialect;

@RealSpannerCompatible
public class BasicSanityCheckGoogleSqlIT extends BasicSanityCheckTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicSanityCheckGoogleSqlIT.class);

    @Test
    public void shouldStreamUpdatesToKafkaGoogleSql() throws InterruptedException {
        shouldStreamUpdatesToKafka(Dialect.GOOGLE_STANDARD_SQL, LOGGER);
    }
}
