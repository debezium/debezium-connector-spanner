/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.function;

/**
 * Represents a supplier of results.
 *
 * <p>There is no requirement that a new or distinct result be returned each
 * time the supplier is invoked.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #get()}.
 *
 * @param <T> the type of results supplied by this supplier
 * @throws InterruptedException if the calling thread is interrupted while blocking
 *
 * @since 1.8
 */
@FunctionalInterface
public interface BlockingSupplier<T> {

    /**
     * Gets a result.
     *
     * @return a result
     */
    T get() throws InterruptedException;
}