/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import java.util.Set;
import java.util.stream.Stream;

/**
 * When merge of partitions happens, it is possible
 * that several tasks will process the same child partition.
 *
 * This class introduces the algorithm to understand,
 * who should be the owner of the partition.
 */
public class ConflictResolver {

    private ConflictResolver() {
    }

    public static boolean hasPriority(String testedTaskUid, Set<String> taskUids) {
        String taskUid = Stream.concat(taskUids.stream(), Stream.of(testedTaskUid)).min(String::compareTo).get();
        return testedTaskUid.equals(taskUid);
    }

    public static String getPriorityPartition(Set<String> tokens) {
        return getPriorityString(tokens);
    }

    private static String getPriorityString(Set<String> strings) {
        return strings.stream().min(String::compareTo).get();
    }
}
