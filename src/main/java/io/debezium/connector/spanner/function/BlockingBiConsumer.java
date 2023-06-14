/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.function;

import java.util.function.BiConsumer;

/**
 * A variant of {@link BiConsumer} that can be blocked and interrupted.
 * @param <T> the type of the input to the operation
 * @author Randall Hauch
 */
@FunctionalInterface
public interface BlockingBiConsumer<T, U> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the first input argument
     * @param u the second input argument
     * @throws InterruptedException if the calling thread is interrupted while blocking
     */
    void accept(T t, U u) throws InterruptedException;
}