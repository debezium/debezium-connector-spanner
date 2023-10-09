/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import org.testcontainers.containers.ContainerState;

import okhttp3.MediaType;

public class SchemaRegistryApi {

    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private final ContainerState containerState;
    private final int kafkaHttpPort;

    public SchemaRegistryApi(ContainerState containerState, int schemaRegistryApiPort) {
        this.containerState = containerState;
        this.kafkaHttpPort = schemaRegistryApiPort;
    }

    private String getAddress() {
        return containerState.getHost() + ":" + containerState.getMappedPort(kafkaHttpPort);
    }

    public String getSchemaRegistryEndpoint() {
        return "http://" + getAddress();
    }
}