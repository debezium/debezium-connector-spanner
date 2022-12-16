/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConflictResolverTest {

    private static final String TOKEN1 = "__8BAYEGwBxGUAABgsBMg1Rlc3RTdHJlYW0yAAGEgQCCgIMIw2QAAAAAAGGEBB2xwRSFZzBfMAAB__-F_wXvfLOHMNiG_wXvfYMq7ISH_wXvfLOHMNjAZAEB__8";
    private static final String TOKEN2 = "__8BAYEGwBxGUAABgsBMg1Rlc3RTdHJlYW0yAAGEgQCCgIMIw2QAAAAAAGKEBB2xwRSFZzBfMAAB__-F_wXvfLOHMNiG_wXvfTMuel2H_wXvfLOHMNjAZAEB__8";

    @Test
    void getPriorityPartition() {
        String token = ConflictResolver.getPriorityPartition(Set.of(TOKEN1, TOKEN2));

        Assertions.assertEquals(TOKEN1, token);
    }
}
