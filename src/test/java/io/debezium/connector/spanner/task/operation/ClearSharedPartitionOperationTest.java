/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Unit tests for {@link ClearSharedPartitionOperation}.
 */
class ClearSharedPartitionOperationTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static PartitionState shared(String token) {
        return PartitionState.builder().token(token).build();
    }

    private static PartitionState partition(String token, PartitionStateEnum state) {
        return PartitionState.builder().token(token).state(state).build();
    }

    private static TaskState taskState(String uid, List<PartitionState> partitions, List<PartitionState> sharedPartitions) {
        return TaskState.builder()
                .taskUid(uid)
                .partitions(partitions)
                .sharedPartitions(sharedPartitions)
                .build();
    }

    private static TaskSyncContext context(TaskState current, TaskState... others) {
        Map<String, TaskState> otherStates = new java.util.HashMap<>();
        for (TaskState ts : others) {
            otherStates.put(ts.getTaskUid(), ts);
        }
        return TaskSyncContext.builder()
                .taskUid(current.getTaskUid())
                .currentTaskState(current)
                .taskStates(otherStates)
                .build();
    }

    private static List<PartitionState> sharedPartitions(TaskSyncContext ctx) {
        return List.copyOf(ctx.getCurrentTaskState().getSharedPartitions());
    }

    private static List<PartitionState> partitions(TaskSyncContext ctx) {
        return List.copyOf(ctx.getCurrentTaskState().getPartitions());
    }

    // -----------------------------------------------------------------------
    // Regression: liveness gap fix
    // -----------------------------------------------------------------------

    /**
     * Regression test for the liveness gap: when two tasks simultaneously hold the same token
     * in their {@code sharedPartitions} (mutable key range race), the current (higher-UID) task
     * must NOT drop its claim just because the lower-UID task also has a sharedPartitions entry.
     *
     * <p>Dropping the claim prematurely created a ~45-second gap: if the lower-UID task crashed
     * before moving the token into its {@code partitions}, no task retained a claim and the
     * partition was unstreamed until the next Kafka rebalance.
     *
     * <p>The fix: only drop a sharedPartitions entry once another task has actually moved the
     * token into its own {@code partitions} list.
     */
    @Test
    void sharedPartitionsClaim_notDropped_whenLowerUidTaskOnlyHasSharedEntry() {
        // "task-A" (lower UID) has the token in sharedPartitions only — has NOT taken it yet.
        TaskState lowerUidTask = taskState("task-A",
                List.of(),
                List.of(shared("token-1")));

        // "task-B" (higher UID, current task) also has the token in sharedPartitions.
        TaskState currentTask = taskState("task-B",
                List.of(),
                List.of(shared("token-1")));

        TaskSyncContext result = new ClearSharedPartitionOperation()
                .doOperation(context(currentTask, lowerUidTask));

        // task-B must keep its claim — task-A hasn't started streaming yet and may crash.
        assertEquals(1, sharedPartitions(result).size(),
                "higher-UID task must retain its sharedPartitions entry while lower-UID task has not yet taken the partition");
        assertEquals("token-1", sharedPartitions(result).get(0).getToken());
    }

    // -----------------------------------------------------------------------
    // Normal cleanup: drop claim once another task owns the partition
    // -----------------------------------------------------------------------

    /**
     * Once any other task has moved the token into its {@code partitions} list (regardless of
     * UID), the current task's sharedPartitions entry is no longer needed and must be removed.
     */
    @Test
    void sharedPartitionsClaim_dropped_whenOtherTaskHasTokenInPartitions() {
        // "task-A" has already taken token-1 into its partitions (actively streaming).
        TaskState otherTask = taskState("task-A",
                List.of(partition("token-1", PartitionStateEnum.RUNNING)),
                List.of());

        TaskState currentTask = taskState("task-B",
                List.of(),
                List.of(shared("token-1")));

        TaskSyncContext result = new ClearSharedPartitionOperation()
                .doOperation(context(currentTask, otherTask));

        assertTrue(sharedPartitions(result).isEmpty(),
                "sharedPartitions entry must be removed once another task has taken ownership");
    }

    /**
     * A FINISHED partition in another task's {@code partitions} list also removes the
     * sharedPartitions claim — the partition has been fully streamed and no longer needs a claimer.
     */
    @Test
    void sharedPartitionsClaim_dropped_whenOtherTaskHasTokenAsFinished() {
        TaskState otherTask = taskState("task-A",
                List.of(partition("token-1", PartitionStateEnum.FINISHED)),
                List.of());

        TaskState currentTask = taskState("task-B",
                List.of(),
                List.of(shared("token-1")));

        TaskSyncContext result = new ClearSharedPartitionOperation()
                .doOperation(context(currentTask, otherTask));

        assertTrue(sharedPartitions(result).isEmpty());
    }

    // -----------------------------------------------------------------------
    // Partition-level self-healing (lowerUidActivePartitionTokens)
    // -----------------------------------------------------------------------

    /**
     * If both tasks have the same token in their {@code partitions} as an active partition
     * (RUNNING), the higher-UID task must mark its copy as REMOVED to resolve the duplicate.
     */
    @Test
    void duplicatePartition_higherUidTaskMarkedRemoved_whenLowerUidTaskAlsoOwnsIt() {
        // "task-A" (lower UID) actively owns token-1.
        TaskState lowerUidTask = taskState("task-A",
                List.of(partition("token-1", PartitionStateEnum.RUNNING)),
                List.of());

        // "task-B" (higher UID, current) also has token-1 as RUNNING — duplicate ownership.
        TaskState currentTask = taskState("task-B",
                List.of(partition("token-1", PartitionStateEnum.RUNNING)),
                List.of());

        TaskSyncContext result = new ClearSharedPartitionOperation()
                .doOperation(context(currentTask, lowerUidTask));

        List<PartitionState> resultPartitions = partitions(result);
        assertEquals(1, resultPartitions.size());
        assertEquals(PartitionStateEnum.REMOVED, resultPartitions.get(0).getState(),
                "higher-UID task must mark its duplicate partition as REMOVED");
    }

    /**
     * A higher-UID task's FINISHED or REMOVED partition is not re-marked — only active
     * (non-FINISHED, non-REMOVED) duplicates are healed.
     */
    @Test
    void duplicatePartition_alreadyFinished_notReMarked() {
        TaskState lowerUidTask = taskState("task-A",
                List.of(partition("token-1", PartitionStateEnum.RUNNING)),
                List.of());

        // Current task's copy is already FINISHED — no healing needed.
        TaskState currentTask = taskState("task-B",
                List.of(partition("token-1", PartitionStateEnum.FINISHED)),
                List.of());

        TaskSyncContext result = new ClearSharedPartitionOperation()
                .doOperation(context(currentTask, lowerUidTask));

        assertEquals(PartitionStateEnum.FINISHED, partitions(result).get(0).getState(),
                "FINISHED partition must not be re-marked by the duplicate-healing logic");
    }

    // -----------------------------------------------------------------------
    // Isolation
    // -----------------------------------------------------------------------

    /** Unrelated tokens in sharedPartitions must be unaffected. */
    @Test
    void unrelatedSharedPartitions_preserved() {
        // Other task owns token-1 (in partitions), but token-2 is unrelated.
        TaskState otherTask = taskState("task-A",
                List.of(partition("token-1", PartitionStateEnum.RUNNING)),
                List.of(shared("token-3")));

        TaskState currentTask = taskState("task-B",
                List.of(),
                List.of(shared("token-1"), shared("token-2")));

        TaskSyncContext result = new ClearSharedPartitionOperation()
                .doOperation(context(currentTask, otherTask));

        List<PartitionState> remaining = sharedPartitions(result);
        assertEquals(1, remaining.size());
        assertEquals("token-2", remaining.get(0).getToken(),
                "token-1 must be removed (owned by other task), token-2 must be kept");
    }

    /** If there are no duplicates, isRequiredPublishSyncEvent must be false. */
    @Test
    void noChanges_publishSyncEventNotRequired() {
        TaskState currentTask = taskState("task-B",
                List.of(),
                List.of(shared("token-1")));

        ClearSharedPartitionOperation op = new ClearSharedPartitionOperation();
        op.doOperation(context(currentTask));

        assertTrue(!op.isRequiredPublishSyncEvent(),
                "no state changed — sync event publish must not be required");
    }
}
