/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.model;

import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.connector.spanner.db.model.event.DataChangeEvent;

/**
 * Represents a modification in a table emitted within a {@link DataChangeEvent}. Each mod contains
 * keys, new values and old values returned as JSON strings.
 */
public class Mod {

    private final JsonNode keysJsonNode;

    private final JsonNode oldValuesJsonNode;
    private final JsonNode newValuesJsonNode;

    private final int modNumber;

    /**
     * Constructs a mod from the primary key values, the old state of the row and the new state of the
     * row.
     *
     * @param modNumber         Modification index number
     *
     * @param keysJsonNode      JSON object as String, where the keys are the primary key column names and the
     *                          values are the primary key column values
     * @param oldValuesJsonNode JSON object as String, displaying the old state of the columns modified.
     *                          This JSON object can be null in the case of an INSERT
     * @param newValuesJsonNode JSON object as String, displaying the new state of the columns modified.
     *                          This JSON object can be null in the case of a DELETE
     */
    public Mod(int modNumber, JsonNode keysJsonNode, JsonNode oldValuesJsonNode, JsonNode newValuesJsonNode) {
        this.modNumber = modNumber;
        this.keysJsonNode = keysJsonNode;
        this.oldValuesJsonNode = oldValuesJsonNode;
        this.newValuesJsonNode = newValuesJsonNode;
    }

    /**
     * The old column values before the modification was applied. This can be null when the
     * modification was emitted for an INSERT operation. The values are returned as a JSON object
     * (stringified), where the keys are the column names and the values are the column values.
     *
     * @return JSON object as String representing the old column values before the row was modified
     */
    public JsonNode oldValuesJsonNode() {
        return oldValuesJsonNode;
    }

    /**
     * The new column values after the modification was applied. This can be null when the
     * modification was emitted for a DELETE operation. The values are returned as a JSON object
     * (stringified), where the keys are the column names and the values are the column values.
     *
     * @return JSON object as String representing the new column values after the row was modified
     */
    public JsonNode newValuesJsonNode() {
        return newValuesJsonNode;
    }

    /**
     * The primary keys of this specific modification. This is always present and can not be null. The
     * keys are returned as a JSON object (stringified), where the keys are the column names and the
     * values are the column values.
     *
     * @return JSON object as String representing the primary key state for the row modified
     */
    public JsonNode keysJsonNode() {
        return keysJsonNode;
    }

    public JsonNode getKeyJsonNode(String name) {
        return keysJsonNode.get(name);
    }

    public JsonNode getNewValueNode(String name) {
        if (this.keysJsonNode.get(name) != null) {
            return this.keysJsonNode.get(name);
        }
        return this.newValuesJsonNode.get(name);
    }

    public JsonNode getOldValueNode(String name) {
        if (this.keysJsonNode.get(name) != null) {
            return this.keysJsonNode.get(name);
        }
        return this.oldValuesJsonNode.get(name);
    }

    public int getModNumber() {
        return modNumber;
    }

    @Override
    public String toString() {
        return "Mod{" +
                "keysJsonNode=" + keysJsonNode +
                ", oldValuesJsonNode=" + oldValuesJsonNode +
                ", newValuesJsonNode=" + newValuesJsonNode +
                ", modNumber=" + modNumber +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Mod mod = (Mod) o;

        if (modNumber != mod.modNumber) {
            return false;
        }
        if (!Objects.equals(keysJsonNode, mod.keysJsonNode)) {
            return false;
        }
        if (!Objects.equals(oldValuesJsonNode, mod.oldValuesJsonNode)) {
            return false;
        }
        return Objects.equals(newValuesJsonNode, mod.newValuesJsonNode);
    }

    @Override
    public int hashCode() {
        int result = keysJsonNode != null ? keysJsonNode.hashCode() : 0;
        result = 31 * result + (oldValuesJsonNode != null ? oldValuesJsonNode.hashCode() : 0);
        result = 31 * result + (newValuesJsonNode != null ? newValuesJsonNode.hashCode() : 0);
        result = 31 * result + modNumber;
        return result;
    }
}
