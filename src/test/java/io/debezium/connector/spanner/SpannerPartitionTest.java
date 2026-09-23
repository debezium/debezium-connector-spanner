/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class SpannerPartitionTest {

    @Test
    void testConstructor() {
        SpannerPartition actualSpannerPartition = new SpannerPartition("test");
        assertEquals("test", actualSpannerPartition.getValue());
        assertNull(actualSpannerPartition.getTvfName());
        assertEquals("SpannerPartition[{partitionToken=test}]", actualSpannerPartition.toString());
    }

    @Test
    void testConstructorWithTvfName() {
        SpannerPartition actualSpannerPartition = new SpannerPartition("test", "tvfA");
        assertEquals("test", actualSpannerPartition.getValue());
        assertEquals("tvfA", actualSpannerPartition.getTvfName());
        assertEquals("SpannerPartition[{partitionToken=test, tvfName=tvfA}]", actualSpannerPartition.toString());
    }

    @Test
    void immutableKeyRangeUsesTokenOnlySourcePartition() {
        Map<String, String> sourcePartition = SpannerPartition.getInitialSpannerPartition().getSourcePartition();

        assertEquals(Map.of("partitionToken", "Parent0"), sourcePartition);
        assertEquals(List.of("partitionToken"), new ArrayList<>(sourcePartition.keySet()));
        assertThrows(UnsupportedOperationException.class, () -> sourcePartition.put("tvfName", "tvfA"));
    }

    @Test
    void mutableKeyRangeWithoutPlacementTvfsUsesTokenOnlySourcePartition() {
        Map<String, String> sourcePartition = new SpannerPartition("token", null).getSourcePartition();

        assertEquals(Map.of("partitionToken", "token"), sourcePartition);
        assertEquals(List.of("partitionToken"), new ArrayList<>(sourcePartition.keySet()));
    }

    @Test
    void blankTvfNameUsesTokenOnlySourcePartition() {
        Map<String, String> sourcePartition = new SpannerPartition("token", "  ").getSourcePartition();

        assertEquals(Map.of("partitionToken", "token"), sourcePartition);
        assertEquals(List.of("partitionToken"), new ArrayList<>(sourcePartition.keySet()));
    }

    @Test
    void mutableKeyRangeWithPlacementTvfUsesOrderedCompositeSourcePartition() {
        Map<String, String> sourcePartition = new SpannerPartition("token", "tvfA").getSourcePartition();

        assertEquals(Map.of("partitionToken", "token", "tvfName", "tvfA"), sourcePartition);
        assertEquals(List.of("partitionToken", "tvfName"), new ArrayList<>(sourcePartition.keySet()));
        assertThrows(UnsupportedOperationException.class, () -> sourcePartition.put("other", "value"));
    }

    @Test
    void testExtractToken() {
        assertNull(SpannerPartition.extractToken(new HashMap<>()));
    }

    @Test
    void testExtractTvfName() {
        assertNull(SpannerPartition.extractTvfName(Map.of("partitionToken", "token")));
        assertEquals("tvfA", SpannerPartition.extractTvfName(Map.of("partitionToken", "token", "tvfName", "tvfA")));
    }

    @Test
    void testGetInitialSpannerPartition() {
        assertEquals("Parent0", SpannerPartition.getInitialSpannerPartition().getValue());
    }

    @Test
    void partitionsWithSameTokenButDifferentTvfAreNotEqual() {
        SpannerPartition a = new SpannerPartition("token", "tvfA");
        SpannerPartition b = new SpannerPartition("token", "tvfB");
        SpannerPartition legacyA = new SpannerPartition("token");
        SpannerPartition legacyB = new SpannerPartition("token", null);

        assertNotEquals(a, b);
        assertNotEquals(a.hashCode(), b.hashCode());
        assertEquals(legacyA, legacyB);
        assertEquals(legacyA.hashCode(), legacyB.hashCode());
    }
}
