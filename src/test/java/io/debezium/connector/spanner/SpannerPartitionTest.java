/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class SpannerPartitionTest {

    @Test
    void testConstructor() {
        SpannerPartition actualSpannerPartition = new SpannerPartition("test");
        assertEquals("test", actualSpannerPartition.getValue());
        assertEquals("SpannerPartition[{partitionToken=test}]", actualSpannerPartition.toString());
    }

    @Test
    void testGetSourcePartition() {
        Map<String, String> actualSourcePartition = SpannerPartition.getInitialSpannerPartition().getSourcePartition();
        assertEquals(1, actualSourcePartition.size());
        assertEquals("Parent0", actualSourcePartition.get("partitionToken"));
    }

    @Test
    void testExtractToken() {
        assertNull(SpannerPartition.extractToken(new HashMap<>()));
    }

    @Test
    void testGetInitialSpannerPartition() {
        assertEquals("Parent0", SpannerPartition.getInitialSpannerPartition().getValue());
    }
}
