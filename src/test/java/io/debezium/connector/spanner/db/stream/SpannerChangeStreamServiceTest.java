/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

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
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.StreamEventMetadata;
import io.debezium.connector.spanner.db.model.event.ChildPartitionsEvent;
import io.debezium.connector.spanner.db.model.event.FinishPartitionEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;
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
    void testBoundedStreamEndingBeforeEndTimestampThrowsException() throws Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet changeStreamResultSet = mock(ChangeStreamResultSet.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);

        // Stream cut off at timestamp 50L, but partition endTimestamp is 100L
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

        ChangeStreamException thrown = assertThrows(ChangeStreamException.class, () -> {
            spannerChangeStreamService.getEvents(partition, changeStreamEventConsumer, partitionEventListener);
        });

        assertTrue(thrown.getMessage().contains("reaching end timestamp"));
        verify(partitionEventListener, times(1)).onRun(partition);
        verify(partitionEventListener, never()).onFinish(partition);
        verify(changeStreamEventConsumer, never()).acceptChangeStreamEvent(any(FinishPartitionEvent.class));
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
}
