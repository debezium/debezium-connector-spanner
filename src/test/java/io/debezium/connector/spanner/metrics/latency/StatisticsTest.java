/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.latency;

import java.time.Duration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StatisticsTest {

    @Test
    void updateAndReset() {
        Statistics statistics = new Statistics(Duration.ofSeconds(10), null);
        statistics.update(148);
        statistics.update(197);
        statistics.update(98);
        statistics.update(47);
        statistics.update(397);
        statistics.update(10);
        statistics.update(298);

        Assertions.assertEquals(170.71428571428572, statistics.getAverageValue());

        Assertions.assertEquals(298, statistics.getLastValue());

        Assertions.assertEquals(397, statistics.getMaxValue());

        Assertions.assertEquals(10, statistics.getMinValue());

        statistics.reset();

        Assertions.assertNull(statistics.getAverageValue());

        Assertions.assertNull(statistics.getLastValue());

        Assertions.assertNull(statistics.getMaxValue());

        Assertions.assertNull(statistics.getMinValue());
    }
}
