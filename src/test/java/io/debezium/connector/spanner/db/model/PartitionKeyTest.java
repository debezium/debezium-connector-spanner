/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class PartitionKeyTest {

    @Test
    void normalizesBlankAndNullTvfNameToNull() {
        PartitionKey withNull = new PartitionKey("token", null);
        PartitionKey withBlank = new PartitionKey("token", "   ");
        PartitionKey withEmpty = new PartitionKey("token", "");

        assertEquals(null, withNull.getTvfName());
        assertEquals(null, withBlank.getTvfName());
        assertEquals(null, withEmpty.getTvfName());
    }

    @Test
    void preservesNonBlankTvfName() {
        PartitionKey key = new PartitionKey("token", "READ_Stream_EU");

        assertEquals("token", key.getToken());
        assertEquals("READ_Stream_EU", key.getTvfName());
    }

    @Test
    void equalsAndHashCodeDistinguishTvf() {
        PartitionKey noTvf = new PartitionKey("token", null);
        PartitionKey tvfA = new PartitionKey("token", "tvfA");
        PartitionKey tvfB = new PartitionKey("token", "tvfB");
        PartitionKey anotherNoTvf = new PartitionKey("token", null);

        assertEquals(noTvf, anotherNoTvf);
        assertEquals(noTvf.hashCode(), anotherNoTvf.hashCode());

        assertNotEquals(tvfA, tvfB);
        assertNotEquals(tvfA, noTvf);
    }

    @Test
    void compareToOrdersByTokenThenTvf() {
        PartitionKey a = new PartitionKey("a", null);
        PartitionKey b = new PartitionKey("b", null);
        PartitionKey aTvf = new PartitionKey("a", "tvf");

        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
        assertTrue(a.compareTo(aTvf) < 0);
        assertTrue(aTvf.compareTo(a) > 0);
        assertEquals(0, a.compareTo(new PartitionKey("a", null)));
    }

    @Test
    void tvfKeysAreSortableForCollections() {
        List<PartitionKey> keys = List.of(
                new PartitionKey("b", "tvfB"),
                new PartitionKey("a", null),
                new PartitionKey("a", "tvfA"));

        List<PartitionKey> sorted = new java.util.ArrayList<>(keys);
        java.util.Collections.sort(sorted);

        assertEquals(List.of(
                new PartitionKey("a", null),
                new PartitionKey("a", "tvfA"),
                new PartitionKey("b", "tvfB")), sorted);
    }
}
