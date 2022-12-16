/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.MissingNode;

class ModTest {

    @Test
    void testConstructor() {
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        Mod actualMod = new Mod(10, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance());
        String actualToStringResult = actualMod.toString();
        assertEquals(10, actualMod.getModNumber());
        assertEquals("Mod{keysJsonNode=, oldValuesJsonNode=, newValuesJsonNode=, modNumber=10}", actualToStringResult);
    }

    @Test
    void testOldValuesJsonNode() {
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        MissingNode instance = MissingNode.getInstance();
        assertSame(instance, new Mod(10, keysJsonNode, oldValuesJsonNode, instance).oldValuesJsonNode());
    }

    @Test
    void testNewValuesJsonNode() {
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        MissingNode instance = MissingNode.getInstance();
        assertSame(instance, new Mod(10, keysJsonNode, oldValuesJsonNode, instance).newValuesJsonNode());
    }

    @Test
    void testKeysJsonNode() {
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        MissingNode instance = MissingNode.getInstance();
        assertSame(instance, new Mod(10, keysJsonNode, oldValuesJsonNode, instance).keysJsonNode());
    }

    @Test
    void testGetKeyJsonNode() {
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        assertNull(new Mod(10, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance()).getKeyJsonNode("Name"));
    }

    @Test
    void testGetNewValueNode() {
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        assertNull(new Mod(10, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance()).getNewValueNode("Name"));
    }

    @Test
    void testGetOldValueNode() {
        MissingNode keysJsonNode = MissingNode.getInstance();
        MissingNode oldValuesJsonNode = MissingNode.getInstance();
        assertNull(new Mod(10, keysJsonNode, oldValuesJsonNode, MissingNode.getInstance()).getOldValueNode("Name"));
    }
}
