/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;

import io.debezium.connector.spanner.db.DatabaseClientFactory;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.StreamEventMetadata;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.stream.exception.ChangeStreamException;
import io.debezium.connector.spanner.db.stream.exception.FailureChangeStreamException;
import io.debezium.connector.spanner.db.stream.exception.OutOfRangeChangeStreamException;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

class SpannerChangeStreamTest {

    @Test
    void testRun() throws ChangeStreamException, InterruptedException {
        SpannerChangeStreamService streamService = mock(SpannerChangeStreamService.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);

        SpannerChangeStream spannerChangeStream = new SpannerChangeStream(streamService, metricsEventPublisher, Duration.ofSeconds(60), 3, "taskUid",
                databaseClientFactory);
        spannerChangeStream.run(() -> false, null, null);
        verify(metricsEventPublisher, times(0)).publishMetricEvent(any());
    }

    @Test
    void testOnStreamEvent() throws ChangeStreamException, InterruptedException {
        SpannerChangeStreamService streamService = mock(SpannerChangeStreamService.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        ChangeStreamEventConsumer changeStreamEventConsumer = mock(ChangeStreamEventConsumer.class);
        ChangeStreamEvent changeStreamEvent = mock(ChangeStreamEvent.class);
        StreamEventMetadata streamEventMetadata = mock(StreamEventMetadata.class);
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);
        when(changeStreamEvent.getMetadata()).thenReturn(streamEventMetadata);
        when(streamEventMetadata.getPartitionToken()).thenReturn("");

        SpannerChangeStream spannerChangeStream = new SpannerChangeStream(streamService, metricsEventPublisher, Duration.ofSeconds(60), 3, "taskUid",
                databaseClientFactory);
        spannerChangeStream.run(() -> false, changeStreamEventConsumer, null);
        spannerChangeStream.onStreamEvent(changeStreamEvent);

        verify(changeStreamEventConsumer).acceptChangeStreamEvent(any());
        verify(changeStreamEvent).getMetadata();
        verify(streamEventMetadata).getPartitionToken();
    }

    @Test
    void testOnStuckPartition() throws ChangeStreamException, InterruptedException {
        SpannerChangeStreamService streamService = mock(SpannerChangeStreamService.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        PartitionEventListener partitionEventListener = mock(PartitionEventListener.class);
        when(partitionEventListener.onStuckPartition(anyString())).thenReturn(true);
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);

        SpannerChangeStream spannerChangeStream = new SpannerChangeStream(streamService, metricsEventPublisher, Duration.ofSeconds(60), 3, "taskUid",
                databaseClientFactory);
        spannerChangeStream.run(() -> false, null, partitionEventListener);
        spannerChangeStream.onStuckPartition("");

        verify(partitionEventListener).onStuckPartition(any());
    }

    @Test
    void testOnError() {
        SpannerChangeStreamService streamService = mock(SpannerChangeStreamService.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        FailureChangeStreamException exception = mock(FailureChangeStreamException.class);
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);

        SpannerChangeStream spannerChangeStream = new SpannerChangeStream(streamService, metricsEventPublisher, Duration.ofSeconds(60), 3, "taskUid",
                databaseClientFactory);
        assertTrue(spannerChangeStream.onError(null, null));
        assertTrue(spannerChangeStream.onError(exception));
    }

    @Test
    void testStop() {
        SpannerChangeStreamService streamService = mock(SpannerChangeStreamService.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);

        SpannerChangeStream spannerChangeStream = new SpannerChangeStream(streamService, metricsEventPublisher, Duration.ofSeconds(60), 3, "taskUid",
                databaseClientFactory);
        spannerChangeStream.stop();
    }

    @Test
    void testIsCanceled() {
        SpannerChangeStreamService streamService = mock(SpannerChangeStreamService.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        SpannerException exception = mock(SpannerException.class);
        when(exception.getErrorCode()).thenReturn(ErrorCode.CANCELLED);
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);

        SpannerChangeStream spannerChangeStream = new SpannerChangeStream(streamService, metricsEventPublisher, Duration.ofSeconds(60), 3, "taskUid",
                databaseClientFactory);
        assertTrue(spannerChangeStream.isCanceled(exception));
        assertFalse(spannerChangeStream.isCanceled(null));
    }

    @Test
    void testGetStreamException() {
        SpannerChangeStreamService streamService = mock(SpannerChangeStreamService.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        SpannerException exception = mock(SpannerException.class);
        Partition partition = mock(Partition.class);
        when(partition.toString()).thenReturn("");
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);

        SpannerChangeStream spannerChangeStream = new SpannerChangeStream(streamService, metricsEventPublisher, Duration.ofSeconds(60), 3, "taskUid",
                databaseClientFactory);
        when(exception.getErrorCode()).thenReturn(ErrorCode.OUT_OF_RANGE);
        assertTrue(spannerChangeStream.getStreamException(partition, exception) instanceof OutOfRangeChangeStreamException);

        when(exception.getErrorCode()).thenReturn(ErrorCode.INVALID_ARGUMENT);
        assertTrue(spannerChangeStream.getStreamException(null, exception) instanceof ChangeStreamException);

        assertTrue(
                spannerChangeStream.getStreamException(null, new NullPointerException()) instanceof ChangeStreamException);
    }

    @Test
    void testSubmitPartition() {
        SpannerChangeStreamService streamService = mock(SpannerChangeStreamService.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        DatabaseClientFactory databaseClientFactory = mock(DatabaseClientFactory.class);

        HashSet<String> parentTokens = new HashSet<>();
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(1L);
        Partition partition = new Partition("partitionToken", parentTokens, startTimestamp, Timestamp.ofTimeMicroseconds(1L), "originParent");

        SpannerChangeStream spannerChangeStream = new SpannerChangeStream(streamService, metricsEventPublisher, Duration.ofSeconds(60), 3, "taskuid",
                databaseClientFactory);

        new Thread(() -> {
            try {
                spannerChangeStream.run(() -> true, changeStreamEvent -> {
                }, new PartitionEventListener() {
                    @Override
                    public void onRun(Partition partition) {

                    }

                    @Override
                    public void onFinish(Partition partition) {

                    }

                    @Override
                    public void onException(Partition partition, Exception ex) {

                    }

                    @Override
                    public boolean onStuckPartition(String token) {
                        return false;
                    }
                });

            }
            catch (ChangeStreamException e) {
                throw new RuntimeException(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> spannerChangeStream.submitPartition(partition));

        verify(metricsEventPublisher, times(2)).publishMetricEvent(any());
    }
}
