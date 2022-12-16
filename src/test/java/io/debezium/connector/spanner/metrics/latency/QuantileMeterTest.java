/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metrics.latency;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Arrays;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

class QuantileMeterTest {

    @Test
    void testGetCount() {
        assertEquals(0.0d, new QuantileMeter(Duration.ofSeconds(10), null).getCount());
    }

    @Test
    void testAddValue() throws InterruptedException {
        QuantileMeter quantileMeter = new QuantileMeter(Duration.ofSeconds(10), (Consumer<Throwable>) mock(Consumer.class));
        assertArrayEquals(new Double[]{ null, null, null }, quantileMeter.getValuesAtQuantiles());
        quantileMeter.addValue(20.0d);
        quantileMeter.start();
        Thread.sleep(1000);
        assertFalse(Arrays.equals(new Double[]{ 0d, 0d, 0d }, quantileMeter.getValuesAtQuantiles()));
    }

    @Test
    void testGetValueAtQuantile() {
        QuantileMeter quantileMeter = new QuantileMeter(
                Duration.ofSeconds(10), (Consumer<Throwable>) mock(Consumer.class));
        assertEquals(null, quantileMeter.getValueAtQuantile(10.0d));
    }

    @Test
    void testReset() {
        QuantileMeter quantileMeter = new QuantileMeter(Duration.ofSeconds(10), (Consumer<Throwable>) mock(Consumer.class));
        quantileMeter.reset();
        assertEquals(0.0d, quantileMeter.getCount());
    }

    @Test
    void testShutdown() {
        QuantileMeter quantileMeter = new QuantileMeter(Duration.ofSeconds(10), (Consumer<Throwable>) mock(Consumer.class));
        quantileMeter.shutdown();
        assertEquals(0.0d, quantileMeter.getCount());
    }
}
