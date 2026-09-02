/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;

import io.debezium.connector.spanner.db.dao.ChangeStreamDao;
import io.debezium.connector.spanner.db.dao.ChangeStreamResultSet;
import io.debezium.connector.spanner.db.dao.ChangeStreamResultSetMetadata;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.StreamEventMetadata;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.event.PartitionEventEvent;
import io.debezium.connector.spanner.db.stream.ChangeStreamEventConsumer;
import io.debezium.connector.spanner.db.stream.MutableStreamOptions;
import io.debezium.connector.spanner.db.stream.PartitionEventListener;
import io.debezium.connector.spanner.db.stream.SpannerChangeStreamService;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.task.TaskSyncContext;
import io.debezium.connector.spanner.task.operation.FindPartitionForStreamingOperation;
import io.debezium.connector.spanner.task.operation.MoveInStateUpdateOperation;
import io.debezium.connector.spanner.task.operation.MoveOutStateUpdateOperation;

/**
 * Unit test simulating placement table partition move-in, move-out, and row mutation records:
 *   - Record 1: MoveOut on P_src at commit TS 1785955138.566892000, seq "83365be2414b3aae-00000001"
 *   - Record 2: MoveIn on P_dst at commit TS 1785955138.566892000, seq "83365be2414b3aae-00000000"
 *   - Record 3: DataChangeRecord (INSERT into BenchmarkPlacementUsers) on P_dst at commit TS 1785955138.566892000, seq "83365be2414b3aae-00000002"
 *
 * Verifies MoveIn waiting/resuming logic, mapping, and duplicate boundary filtering for placement tables.
 */
class PlacementMoveInMoveOutTest {

    private static final String P_SRC = "__8BAYEHAiwAH5AAAYLAQYNteWNzAAGEgQYlWTAQARGCgIMAhATIEK0yhWcyNzNfMzkxNjI2MjUAAf__wGQBAf__";
    private static final String P_DST = "__8BAYEHAiwAH5AAAYLAQYNteWNzAAGEgQYlWS_gARGCgIMAhAS4rvlzhWcyNzNfMzkxNjI2MjIAAf__wGQBAf__";
    private static final Timestamp TS = Timestamp.ofTimeSecondsAndNanos(1785955138L, 566892000);

    private TaskSyncContext context;

    private com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord buildRecord1MoveOutProto() {
        return com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.newBuilder()
                .setCommitTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(1785955138L).setNanos(566892000))
                .setRecordSequence("83365be2414b3aae-00000001")
                .setPartitionToken(P_SRC)
                .addMoveOutEvents(
                        com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.MoveOutEvent.newBuilder()
                                .setDestinationPartitionToken(P_DST))
                .build();
    }

    private com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord buildRecord2MoveInProto() {
        return com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.newBuilder()
                .setCommitTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(1785955138L).setNanos(566892000))
                .setRecordSequence("83365be2414b3aae-00000000")
                .setPartitionToken(P_DST)
                .addMoveInEvents(
                        com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.MoveInEvent.newBuilder()
                                .setSourcePartitionToken(P_SRC))
                .build();
    }

    private com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord buildRecord3DataChangeProto() {
        return com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.newBuilder()
                .setCommitTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(1785955138L).setNanos(566892000))
                .setRecordSequence("83365be2414b3aae-00000002")
                .setServerTransactionId("MTQyNTcxODE0NTc5NzQxNDkyNjk=")
                .setIsLastRecordInTransactionInPartition(true)
                .setTable("BenchmarkPlacementUsers")
                .addColumnMetadata(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata.newBuilder()
                        .setName("UserId").setType(com.google.spanner.v1.Type.newBuilder().setCode(com.google.spanner.v1.TypeCode.INT64)).setIsPrimaryKey(true)
                        .setOrdinalPosition(1))
                .addColumnMetadata(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata.newBuilder()
                        .setName("PlacementKey").setType(com.google.spanner.v1.Type.newBuilder().setCode(com.google.spanner.v1.TypeCode.STRING)).setOrdinalPosition(2))
                .addColumnMetadata(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata.newBuilder()
                        .setName("UserName").setType(com.google.spanner.v1.Type.newBuilder().setCode(com.google.spanner.v1.TypeCode.STRING)).setOrdinalPosition(3))
                .addColumnMetadata(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata.newBuilder()
                        .setName("UserEmail").setType(com.google.spanner.v1.Type.newBuilder().setCode(com.google.spanner.v1.TypeCode.STRING)).setOrdinalPosition(4))
                .addColumnMetadata(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata.newBuilder()
                        .setName("AccountBalance").setType(com.google.spanner.v1.Type.newBuilder().setCode(com.google.spanner.v1.TypeCode.NUMERIC)).setOrdinalPosition(5))
                .addColumnMetadata(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata.newBuilder()
                        .setName("Metadata").setType(com.google.spanner.v1.Type.newBuilder().setCode(com.google.spanner.v1.TypeCode.JSON)).setOrdinalPosition(6))
                .addColumnMetadata(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata.newBuilder()
                        .setName("LastLogin").setType(com.google.spanner.v1.Type.newBuilder().setCode(com.google.spanner.v1.TypeCode.TIMESTAMP)).setOrdinalPosition(7))
                .addColumnMetadata(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata.newBuilder()
                        .setName("IsActive").setType(com.google.spanner.v1.Type.newBuilder().setCode(com.google.spanner.v1.TypeCode.BOOL)).setOrdinalPosition(8))
                .addColumnMetadata(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata.newBuilder()
                        .setName("BinarySignature").setType(com.google.spanner.v1.Type.newBuilder().setCode(com.google.spanner.v1.TypeCode.BYTES)).setOrdinalPosition(9))
                .addMods(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.Mod.newBuilder()
                        .addKeys(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModValue.newBuilder()
                                .setValue(com.google.protobuf.Value.newBuilder().setStringValue("4").build()))
                        .addNewValues(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModValue.newBuilder()
                                .setColumnMetadataIndex(1)
                                .setValue(com.google.protobuf.Value.newBuilder().setStringValue("default").build())))
                .setModType(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModType.INSERT)
                .setValueCaptureType(com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ValueCaptureType.OLD_AND_NEW_VALUES)
                .setNumberOfRecordsInTransaction(1)
                .setNumberOfPartitionsInTransaction(2)
                .build();
    }

    @Test
    void testRecordMapping() {
        DatabaseClient gsqlClient = mock(DatabaseClient.class);
        when(gsqlClient.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
        ChangeStreamRecordMapper mapper = new ChangeStreamRecordMapper(gsqlClient, true);
        ChangeStreamResultSetMetadata metadata = mock(ChangeStreamResultSetMetadata.class);
        when(metadata.getTotalStreamDuration()).thenReturn(org.joda.time.Duration.ZERO);

        // Map Record 1 (MoveOut)
        PartitionEventEvent moveOutEvent = mapper.toPartitionEventEvent(
                new Partition(P_SRC, Set.of(), TS, null, "origin"),
                buildRecord1MoveOutProto(),
                metadata);
        assertEquals(P_SRC, moveOutEvent.getPartitionToken());
        assertEquals(List.of(P_DST), moveOutEvent.getDestinationPartitions());
        assertEquals("83365be2414b3aae-00000001", moveOutEvent.getRecordSequence());

        // Map Record 2 (MoveIn)
        PartitionEventEvent moveInEvent = mapper.toPartitionEventEvent(
                new Partition(P_DST, Set.of(), TS, null, "origin"),
                buildRecord2MoveInProto(),
                metadata);
        assertEquals(P_DST, moveInEvent.getPartitionToken());
        assertEquals(List.of(P_SRC), moveInEvent.getSourcePartitions());
        assertEquals("83365be2414b3aae-00000000", moveInEvent.getRecordSequence());

        // Map Record 3 (DataChange INSERT into BenchmarkPlacementUsers)
        DataChangeEvent dataChangeEvent = mapper.toDataChangeEvent(
                new Partition(P_DST, Set.of(), TS, null, "origin"),
                buildRecord3DataChangeProto(),
                metadata);
        assertEquals("BenchmarkPlacementUsers", dataChangeEvent.getTableName());
        assertEquals("83365be2414b3aae-00000002", dataChangeEvent.getRecordSequence());
        assertFalse(dataChangeEvent.getRowType().stream().anyMatch(col -> "PlacementKey".equals(col.getName()) && col.isPrimaryKey()),
                "PlacementKey must not be marked as a primary key column in rowType");
    }

    @Test
    void testMoveInMoveOutTaskStateSynchronization() {
        // Seed initial partition states
        context = TaskSyncContext.builder()
                .taskUid("task0")
                .currentTaskState(TaskState.builder()
                        .taskUid("task0")
                        .partitions(List.of(
                                PartitionState.builder().token(P_SRC).state(PartitionStateEnum.RUNNING).parents(Set.of()).build(),
                                PartitionState.builder().token(P_DST).state(PartitionStateEnum.RUNNING).parents(Set.of()).build()))
                        .sharedPartitions(List.of())
                        .build())
                .build();

        // 1. P_dst processes MoveIn (Record 2: seq 00000000)
        context = new MoveInStateUpdateOperation(P_DST, TS, "83365be2414b3aae-00000000", List.of(P_SRC)).doOperation(context);
        context = new FindPartitionForStreamingOperation().doOperation(context);

        PartitionState pDstState1 = getPartition(P_DST);
        assertEquals(PartitionStateEnum.CREATED, pDstState1.getState(), "P_dst must pause in CREATED state waiting for P_src MoveOut");
        assertNotNull(pDstState1.getMoveInState());
        assertEquals("83365be2414b3aae-00000000", pDstState1.getMoveInState().getRecordSequence());
        assertEquals(TS, pDstState1.getProcessedTimestamp());

        // 2. P_src processes MoveOut (Record 1: seq 00000001)
        context = new MoveOutStateUpdateOperation(P_SRC, TS, List.of(P_DST)).doOperation(context);
        context = new FindPartitionForStreamingOperation().doOperation(context);

        PartitionState pDstState2 = getPartition(P_DST);
        assertEquals(PartitionStateEnum.READY_FOR_STREAMING, pDstState2.getState(), "P_dst must become READY_FOR_STREAMING after P_src MoveOut");
        assertEquals(TS, pDstState2.getProcessedTimestamp());
        assertEquals("83365be2414b3aae-00000000", pDstState2.getLastBoundaryRecordSequence());
    }

    @Test
    void testMoveInReExecutionLoopBug() throws Exception {
        ChangeStreamDao changeStreamDao = mock(ChangeStreamDao.class);
        ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
        MetricsEventPublisher metricsEventPublisher = mock(MetricsEventPublisher.class);
        DatabaseClient gsqlClient = mock(DatabaseClient.class);
        ChangeStreamRecordMapper mapper = mock(ChangeStreamRecordMapper.class);
        ChangeStreamRecordMapper realMapper = new ChangeStreamRecordMapper(gsqlClient, true);
        ChangeStreamResultSetMetadata metadata = mock(ChangeStreamResultSetMetadata.class);
        when(metadata.getTotalStreamDuration()).thenReturn(org.joda.time.Duration.ZERO);

        StreamEventMetadata meta = StreamEventMetadata.newBuilder().withPartitionToken(P_DST).build();
        PartitionEventEvent moveInEvent = realMapper.toPartitionEventEvent(
                new Partition(P_DST, Set.of(), TS, null, "origin"),
                buildRecord2MoveInProto(),
                metadata);
        DataChangeEvent dataChangeEvent = realMapper.toDataChangeEvent(
                new Partition(P_DST, Set.of(), TS, null, "origin"),
                buildRecord3DataChangeProto(),
                metadata);
        when(changeStreamDao.isMutableKeyRange()).thenReturn(true);
        when(changeStreamDao.streamQuery(any(), any(), any(), anyLong())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getMetadata()).thenReturn(metadata);

        Partition resumedPartition = new Partition(P_DST, Set.of(P_SRC), TS, TS, "origin", "83365be2414b3aae-00000000");

        SpannerChangeStreamService service = new SpannerChangeStreamService(
                "task0", changeStreamDao, mapper, java.time.Duration.ofMinutes(1), metricsEventPublisher,
                20, MutableStreamOptions.withDefaults());

        ChangeStreamEventConsumer consumer = mock(ChangeStreamEventConsumer.class);
        PartitionEventListener listener = mock(PartitionEventListener.class);

        when(mapper.toChangeStreamEvents(any(Partition.class), any(ChangeStreamResultSet.class), any(ChangeStreamResultSetMetadata.class)))
                .thenReturn(List.of(moveInEvent), List.of(dataChangeEvent));

        service.getEvents(resumedPartition, consumer, listener);

        org.mockito.ArgumentCaptor<io.debezium.connector.spanner.db.model.event.ChangeStreamEvent> captor = org.mockito.ArgumentCaptor
                .forClass(io.debezium.connector.spanner.db.model.event.ChangeStreamEvent.class);
        verify(listener, never()).onMoveIn(any(), any(), any(), any());
        verify(consumer, org.mockito.Mockito.times(2)).acceptChangeStreamEvent(captor.capture());
        assertTrue(captor.getAllValues().stream().anyMatch(e -> e instanceof DataChangeEvent && "83365be2414b3aae-00000002".equals(e.getRecordSequence())),
                "DataChangeEvent with sequence 83365be2414b3aae-00000002 must be emitted to consumer");
    }

    private PartitionState getPartition(String token) {
        return context.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getToken().equals(token))
                .findFirst()
                .orElseThrow();
    }
}
