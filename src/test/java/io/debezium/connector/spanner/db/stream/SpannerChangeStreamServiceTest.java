/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.stream;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashSet;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.dao.ChangeStreamDao;
import io.debezium.connector.spanner.db.dao.ChangeStreamResultSet;
import io.debezium.connector.spanner.db.mapper.ChangeStreamRecordMapper;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

class SpannerChangeStreamServiceTest {

    @Test
    void testGetEvents() throws InterruptedException {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet changeStreamResultSet = mock(ChangeStreamResultSet.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        when(changeStreamResultSet.next()).thenReturn(false);
        when(changeStreamDao.streamQuery(any(), any(), any(), anyLong())).thenReturn(changeStreamResultSet);

        SpannerChangeStreamService spannerChangeStreamService = new SpannerChangeStreamService(changeStreamDao,
                new ChangeStreamRecordMapper(), Duration.ofMillis(1000), metricsEventPublisher);
        HashSet<String> parentTokens = new HashSet<>();
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(1L);
        Partition partition = new Partition("token", parentTokens, startTimestamp, Timestamp.ofTimeMicroseconds(1L), "originParent");

        ChangeStreamEventConsumer changeStreamEventConsumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener partitionEventListener = mock(PartitionEventListener.class);
        doNothing().when(partitionEventListener).onRun(any());
        spannerChangeStreamService.getEvents(partition, changeStreamEventConsumer, partitionEventListener);

        verify(changeStreamEventConsumer).acceptChangeStreamEvent(any());
    }
}
