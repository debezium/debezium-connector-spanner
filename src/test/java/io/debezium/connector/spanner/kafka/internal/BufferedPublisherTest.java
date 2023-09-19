/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Ordering;

import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.task.TaskSyncContext;
import io.debezium.connector.spanner.task.TaskSyncContextHolder;

class BufferedPublisherTest {

    @Test
    void testBufferedPublisher_1() throws InterruptedException {
        Predicate<Integer> publishImmediately = p -> p % 10 == 0;

        Consumer<Integer> onPublish = v -> {
        };

        runAndCheck(publishImmediately, onPublish);
    }

    @Test
    void testBufferedPublisher_2() throws InterruptedException {
        Predicate<Integer> publishImmediately = p -> p % 10 == 0;

        Consumer<Integer> onPublish = v -> {
        };

        runAndCheck(publishImmediately, onPublish);
    }

    @Test
    void testBufferedPublisher_3() throws InterruptedException {
        Predicate<Integer> publishImmediately = p -> p % 100 == 0;

        Consumer<Integer> onPublish = v -> {
            try {
                Thread.sleep(20);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        };

        runAndCheck(publishImmediately, onPublish);
    }

    private void runAndCheck(Predicate<Integer> publishImmediately, Consumer<Integer> onPublish) throws InterruptedException {
        List<Integer> result = new CopyOnWriteArrayList<>();

        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        TaskSyncContextHolder taskSyncContextHolder = new TaskSyncContextHolder(metricsEventPublisher);
        taskSyncContextHolder.init(TaskSyncContext.builder().taskUid("test-task-1")
                .rebalanceState(RebalanceState.NEW_EPOCH_STARTED)
                .build());

        BufferedPublisher<Integer> pub = new BufferedPublisher<>("test-task-1", "pub-1", taskSyncContextHolder, 5,
                publishImmediately,
                onPublish
                        // .andThen(System.out::println)
                        .andThen(result::add));

        pub.start();

        int total = 1001;
        List<Integer> required = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            pub.buffer(i);
            Thread.sleep(1);
            if (publishImmediately.test(i)) {
                required.add(i);
            }
        }
        pub.close();

        // System.out.println("published: " + result.size());
        assertThat(result)
                .containsAll(required) // no missed required elements
                .hasSizeGreaterThan(required.size()) // not only required elements
                .hasSizeLessThan(total) // some of not required haven't been published
                .hasSameSizeAs(new HashSet<>(result)); // no duplicates
        assertThat(Ordering.natural().isOrdered(result)).isTrue(); // correctly ordered
    }
}
