/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.dao.ChangeStreamDao;
import io.debezium.connector.spanner.db.dao.ChangeStreamResultSet;
import io.debezium.connector.spanner.db.mapper.ChangeStreamRecordMapper;
import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.StreamEventMetadata;
import io.debezium.connector.spanner.db.model.event.ChildPartitionsEvent;
import io.debezium.connector.spanner.db.model.event.FinishPartitionEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEndEvent;
import io.debezium.connector.spanner.db.stream.exception.ChangeStreamException;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

class SpannerChangeStreamServiceTest {

    @Test
    void testGetEvents() throws InterruptedException, Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet changeStreamResultSet = mock(ChangeStreamResultSet.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);

        Timestamp endTimestamp = Timestamp.ofTimeMicroseconds(100L);
        HeartbeatEvent heartbeatEvent = new HeartbeatEvent(endTimestamp, mock(StreamEventMetadata.class));
        when(changeStreamResultSet.next()).thenReturn(true, false);
        when(changeStreamDao.streamQuery(any(), any(), any(), anyLong())).thenReturn(changeStreamResultSet);
        when(mapper.toChangeStreamEvents(any(), any(), any())).thenReturn(List.of(heartbeatEvent));

        SpannerChangeStreamService spannerChangeStreamService = new SpannerChangeStreamService("TaskUid", changeStreamDao,
                mapper, Duration.ofMillis(1000), metricsEventPublisher);

        HashSet<String> parentTokens = new HashSet<>();
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(1L);
        Partition partition = new Partition("token", parentTokens, startTimestamp, endTimestamp, "originParent");

        ChangeStreamEventConsumer changeStreamEventConsumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener partitionEventListener = mock(PartitionEventListener.class);
        doNothing().when(partitionEventListener).onRun(any());
        doNothing().when(partitionEventListener).onFinish(any());

        spannerChangeStreamService.getEvents(partition, changeStreamEventConsumer, partitionEventListener);

        verify(changeStreamEventConsumer).acceptChangeStreamEvent(heartbeatEvent);
        verify(changeStreamEventConsumer).acceptChangeStreamEvent(any(FinishPartitionEvent.class));
        verify(partitionEventListener).onFinish(partition);
    }

    @Test
    void testStreamEndingWithoutChildPartitionsThrowsException() throws Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet changeStreamResultSet = mock(ChangeStreamResultSet.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);

        // Simulate 1 heartbeat event, then stream EOF (false)
        when(changeStreamResultSet.next()).thenReturn(true, false);
        when(changeStreamDao.streamQuery(any(), any(), any(), anyLong())).thenReturn(changeStreamResultSet);

        HeartbeatEvent heartbeatEvent = new HeartbeatEvent(Timestamp.now(), mock(StreamEventMetadata.class));
        when(mapper.toChangeStreamEvents(any(), any(), any())).thenReturn(List.of(heartbeatEvent));

        SpannerChangeStreamService spannerChangeStreamService = new SpannerChangeStreamService("TaskUid", changeStreamDao,
                mapper, Duration.ofMillis(1000), metricsEventPublisher);

        // Partition with null endTimestamp (continuous CDC partition query)
        HashSet<String> parentTokens = new HashSet<>();
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(1L);
        Partition partition = new Partition("token", parentTokens, startTimestamp, null, "originParent");

        ChangeStreamEventConsumer changeStreamEventConsumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener partitionEventListener = mock(PartitionEventListener.class);
        doNothing().when(partitionEventListener).onRun(any());

        ChangeStreamException thrown = assertThrows(ChangeStreamException.class, () -> {
            spannerChangeStreamService.getEvents(partition, changeStreamEventConsumer, partitionEventListener);
        });

        assertTrue(thrown.getMessage().contains("without child partitions"));
        verify(partitionEventListener, times(1)).onRun(partition);
        verify(partitionEventListener, never()).onFinish(partition);
        verify(changeStreamEventConsumer, never()).acceptChangeStreamEvent(any(FinishPartitionEvent.class));
    }

    @Test
    void testBoundedStreamFinishesCleanlyWithoutChildPartitions() throws Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet changeStreamResultSet = mock(ChangeStreamResultSet.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);

        // Bounded stream ending at timestamp 50L (< partition endTimestamp 100L)
        Timestamp eventTimestamp = Timestamp.ofTimeMicroseconds(50L);
        HeartbeatEvent heartbeatEvent = new HeartbeatEvent(eventTimestamp, mock(StreamEventMetadata.class));
        when(changeStreamResultSet.next()).thenReturn(true, false);
        when(changeStreamDao.streamQuery(any(), any(), any(), anyLong())).thenReturn(changeStreamResultSet);
        when(mapper.toChangeStreamEvents(any(), any(), any())).thenReturn(List.of(heartbeatEvent));

        SpannerChangeStreamService spannerChangeStreamService = new SpannerChangeStreamService("TaskUid", changeStreamDao,
                mapper, Duration.ofMillis(1000), metricsEventPublisher);

        HashSet<String> parentTokens = new HashSet<>();
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(1L);
        Timestamp endTimestamp = Timestamp.ofTimeMicroseconds(100L);
        Partition partition = new Partition("token", parentTokens, startTimestamp, endTimestamp, "originParent");

        ChangeStreamEventConsumer changeStreamEventConsumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener partitionEventListener = mock(PartitionEventListener.class);
        doNothing().when(partitionEventListener).onRun(any());
        doNothing().when(partitionEventListener).onFinish(any());

        spannerChangeStreamService.getEvents(partition, changeStreamEventConsumer, partitionEventListener);

        verify(changeStreamEventConsumer).acceptChangeStreamEvent(heartbeatEvent);
        verify(changeStreamEventConsumer).acceptChangeStreamEvent(any(FinishPartitionEvent.class));
        verify(partitionEventListener).onFinish(partition);
    }

    @Test
    void testBoundedStreamReceivingChildPartitionsFinishesCleanly() throws Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet changeStreamResultSet = mock(ChangeStreamResultSet.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);

        // Partition split before endTimestamp (at timestamp 50L)
        Timestamp eventTimestamp = Timestamp.ofTimeMicroseconds(50L);
        ChildPartitionsEvent childPartitionsEvent = mock(ChildPartitionsEvent.class);
        when(childPartitionsEvent.getRecordTimestamp()).thenReturn(eventTimestamp);
        when(childPartitionsEvent.getMetadata()).thenReturn(mock(StreamEventMetadata.class));

        when(changeStreamResultSet.next()).thenReturn(true, false);
        when(changeStreamDao.streamQuery(any(), any(), any(), anyLong())).thenReturn(changeStreamResultSet);
        when(mapper.toChangeStreamEvents(any(), any(), any())).thenReturn(List.of(childPartitionsEvent));

        SpannerChangeStreamService spannerChangeStreamService = new SpannerChangeStreamService("TaskUid", changeStreamDao,
                mapper, Duration.ofMillis(1000), metricsEventPublisher);

        HashSet<String> parentTokens = new HashSet<>();
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(1L);
        Timestamp endTimestamp = Timestamp.ofTimeMicroseconds(100L);
        Partition partition = new Partition("token", parentTokens, startTimestamp, endTimestamp, "originParent");

        ChangeStreamEventConsumer changeStreamEventConsumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener partitionEventListener = mock(PartitionEventListener.class);
        doNothing().when(partitionEventListener).onRun(any());
        doNothing().when(partitionEventListener).onFinish(any());

        spannerChangeStreamService.getEvents(partition, changeStreamEventConsumer, partitionEventListener);

        verify(changeStreamEventConsumer).acceptChangeStreamEvent(childPartitionsEvent);
        verify(changeStreamEventConsumer).acceptChangeStreamEvent(any(FinishPartitionEvent.class));
        verify(partitionEventListener).onFinish(partition);
    }

    @Test
    void testGetEventsMutableRoutesToMutablePath() throws Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet changeStreamResultSet = mock(ChangeStreamResultSet.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);

        when(changeStreamDao.isMutableKeyRange()).thenReturn(true);
        when(changeStreamResultSet.next()).thenReturn(false);
        when(changeStreamDao.streamQuery(any(), any(), any(), anyLong())).thenReturn(changeStreamResultSet);
        when(mapper.toChangeStreamEvents(any(), any(), any())).thenReturn(List.of());

        SpannerChangeStreamService service = new SpannerChangeStreamService(
                "TaskUid", changeStreamDao, mapper, Duration.ofMillis(1000), metricsEventPublisher);

        Timestamp start = Timestamp.ofTimeSecondsAndNanos(0, 0);
        Timestamp end = Timestamp.ofTimeSecondsAndNanos(0, 0);
        Partition partition = new Partition("token", new HashSet<>(), start, end, "origin");

        ChangeStreamEventConsumer consumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener listener = mock(PartitionEventListener.class);
        doNothing().when(listener).onRun(any());

        service.getEvents(partition, consumer, listener);

        verify(listener).onRun(partition);
        verify(listener).onFinish(partition);
        verify(consumer).acceptChangeStreamEvent(any(FinishPartitionEvent.class));
    }

    @Test
    void testGetEventsMutableWithNullEndTimestampStreamsUntilPartitionEnd() throws Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);

        when(changeStreamDao.isMutableKeyRange()).thenReturn(true);
        when(changeStreamDao.streamQuery(any(), any(), any(), anyLong())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);

        Timestamp start = Timestamp.ofTimeSecondsAndNanos(0, 0);
        StreamEventMetadata meta = StreamEventMetadata.newBuilder()
                .withPartitionToken("token")
                .build();
        PartitionEndEvent endEvent = new PartitionEndEvent(
                start, "seq", "token", meta);
        when(mapper.toChangeStreamEvents(any(), any(), any())).thenReturn(List.of(endEvent));

        SpannerChangeStreamService service = new SpannerChangeStreamService(
                "TaskUid", changeStreamDao, mapper, Duration.ofMillis(1000), metricsEventPublisher);

        Partition partition = new Partition("token", new HashSet<>(), start, null, "origin");
        ChangeStreamEventConsumer consumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener listener = mock(PartitionEventListener.class);
        doNothing().when(listener).onRun(any());

        service.getEvents(partition, consumer, listener);

        org.mockito.ArgumentCaptor<Timestamp> endTsCaptor = org.mockito.ArgumentCaptor.forClass(Timestamp.class);
        verify(changeStreamDao, times(1)).streamQuery(any(), any(), endTsCaptor.capture(), anyLong());
        Timestamp expectedWindowEnd = Timestamp.ofTimeSecondsAndNanos(20 * 60, 0);
        assertEquals(expectedWindowEnd, endTsCaptor.getValue());
        verify(listener).onFinish(partition);
        verify(consumer).acceptChangeStreamEvent(any(FinishPartitionEvent.class));
    }

    @Test
    void testGetEventsMutableStopsOnPartitionEndEvent() throws Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);

        when(changeStreamDao.isMutableKeyRange()).thenReturn(true);
        when(changeStreamDao.streamQuery(any(), any(), any(), anyLong())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);

        StreamEventMetadata meta = StreamEventMetadata.newBuilder()
                .withPartitionToken("token")
                .build();
        PartitionEndEvent endEvent = new PartitionEndEvent(
                Timestamp.ofTimeSecondsAndNanos(0, 0), "seq", "token", meta);

        Partition partition = new Partition("token", new HashSet<>(),
                Timestamp.ofTimeSecondsAndNanos(0, 0),
                Timestamp.ofTimeSecondsAndNanos(3600, 0),
                "origin");

        when(mapper.toChangeStreamEvents(any(), any(), any())).thenReturn(List.of(endEvent));

        SpannerChangeStreamService service = new SpannerChangeStreamService(
                "TaskUid", changeStreamDao, mapper, Duration.ofMillis(1000), metricsEventPublisher);

        ChangeStreamEventConsumer consumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener listener = mock(PartitionEventListener.class);
        doNothing().when(listener).onRun(any());

        service.getEvents(partition, consumer, listener);

        verify(changeStreamDao, times(1)).streamQuery(any(), any(), any(), anyLong());
        verify(listener).onFinish(partition);
    }

    @Test
    void testGetEventsMutableInitialPartitionExitsAfterFirstIteration() throws Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);

        when(changeStreamDao.isMutableKeyRange()).thenReturn(true);
        when(changeStreamDao.streamQuery(any(), any(), any(), anyLong())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);
        when(mapper.toChangeStreamEvents(any(), any(), any())).thenReturn(List.of());

        Timestamp start = Timestamp.ofTimeSecondsAndNanos(0, 0);
        Timestamp end = Timestamp.ofTimeSecondsAndNanos(7200, 0);
        Partition partition = new Partition(
                InitialPartition.PARTITION_TOKEN, new HashSet<>(), start, end, "origin");

        SpannerChangeStreamService service = new SpannerChangeStreamService(
                "TaskUid", changeStreamDao, mapper, Duration.ofMillis(1000),
                mock(MetricsEventPublisher.class));

        ChangeStreamEventConsumer consumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener listener = mock(PartitionEventListener.class);
        doNothing().when(listener).onRun(any());

        service.getEvents(partition, consumer, listener);

        verify(changeStreamDao, times(1)).streamQuery(any(), any(), any(), anyLong());
    }

    @Test
    void testGetEventsMutableDeduplicatesBoundaryEventsAcrossWindows() throws Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet resultSet1 = mock(ChangeStreamResultSet.class);
        ChangeStreamResultSet resultSet2 = mock(ChangeStreamResultSet.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);

        when(changeStreamDao.isMutableKeyRange()).thenReturn(true);

        Timestamp start = Timestamp.ofTimeSecondsAndNanos(0, 0);
        Timestamp end = Timestamp.ofTimeSecondsAndNanos(1200, 0);

        when(changeStreamDao.streamQuery(any(), org.mockito.ArgumentMatchers.eq(start), any(Timestamp.class), anyLong()))
                .thenReturn(resultSet1);
        when(changeStreamDao.streamQuery(any(), org.mockito.ArgumentMatchers.eq(end), any(Timestamp.class), anyLong()))
                .thenReturn(resultSet2);

        when(resultSet1.next()).thenReturn(true, false);
        when(resultSet2.next()).thenReturn(true, false);

        StreamEventMetadata meta = StreamEventMetadata.newBuilder().withPartitionToken("token").build();

        io.debezium.connector.spanner.db.model.event.DataChangeEvent eventAtBoundary = new io.debezium.connector.spanner.db.model.event.DataChangeEvent(
                "token", end, "tx1", true, "00001", "MyTable",
                List.of(), List.of(), io.debezium.connector.spanner.db.model.ModType.INSERT,
                io.debezium.connector.spanner.db.model.ValueCaptureType.NEW_ROW,
                1L, 1L, "", false, meta);

        when(mapper.toChangeStreamEvents(any(), org.mockito.ArgumentMatchers.eq(resultSet1), any()))
                .thenReturn(List.of(eventAtBoundary));
        when(mapper.toChangeStreamEvents(any(), org.mockito.ArgumentMatchers.eq(resultSet2), any()))
                .thenReturn(List.of(eventAtBoundary));

        ChangeStreamEventConsumer consumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener listener = mock(PartitionEventListener.class);
        doNothing().when(listener).onRun(any());

        SpannerChangeStreamService service = new SpannerChangeStreamService(
                "TaskUid", changeStreamDao, mapper, Duration.ofMillis(1000),
                mock(MetricsEventPublisher.class));

        Partition partition = new Partition("token", new HashSet<>(), start, end, "origin");
        service.getEvents(partition, consumer, listener);

        verify(consumer, org.mockito.Mockito.times(1)).acceptChangeStreamEvent(
                org.mockito.ArgumentMatchers.eq(eventAtBoundary));
    }

    @Test
    void testGetEventsImmutablePathUsedWhenNotMutable() throws Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);

        when(changeStreamDao.isMutableKeyRange()).thenReturn(false);
        when(changeStreamDao.streamQuery(any(), any(), any(), anyLong())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        SpannerChangeStreamService service = new SpannerChangeStreamService(
                "TaskUid", changeStreamDao, mapper, Duration.ofMillis(1000),
                mock(MetricsEventPublisher.class));

        Partition partition = new Partition("token", new HashSet<>(),
                Timestamp.ofTimeMicroseconds(1L), null, "origin");

        ChangeStreamEventConsumer consumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener listener = mock(PartitionEventListener.class);
        doNothing().when(listener).onRun(any());

        service.getEvents(partition, consumer, listener);

        verify(consumer).acceptChangeStreamEvent(any(FinishPartitionEvent.class));
        verify(changeStreamDao, times(1)).streamQuery(any(), any(), any(), anyLong());
    }
}
