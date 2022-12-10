/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.context.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.context.offset.LowWatermarkProvider;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.db.model.ModType;
import io.debezium.connector.spanner.db.model.StreamEventMetadata;
import io.debezium.connector.spanner.db.model.ValueCaptureType;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.schema.Column;

class SourceInfoFactoryTest {

    @Test
    void testGetSourceInfoOldAndNewValues() throws InterruptedException {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        when(configuration.getString(anyString())).thenReturn("String");
        SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(configuration);
        SourceInfoFactory sourceInfoFactory = new SourceInfoFactory(
                connectorConfig, mock(LowWatermarkProvider.class));
        StreamEventMetadata streamEventMetadata = mock(StreamEventMetadata.class);
        when(streamEventMetadata.getRecordReadAt()).thenReturn(Timestamp.ofTimeMicroseconds(1L));
        ArrayList<Column> rowType = new ArrayList<>();

        Timestamp commitTimestamp = Timestamp.ofTimeMicroseconds(1L);
        Instant recordTimestamp = commitTimestamp.toSqlTimestamp().toInstant();
        Instant readAtTimestamp = commitTimestamp.toSqlTimestamp().toInstant();
        String serverTransactionId = "testId";
        Long recordSequence = 1L;
        Instant lowWatermark = null;
        Long numberRecordsInTransaction = 1L;

        DataChangeEvent dataChangeEvent = new DataChangeEvent("token", commitTimestamp,
                "testId", true,
                "1", "Table Name", rowType, new ArrayList<>(), ModType.INSERT,
                ValueCaptureType.OLD_AND_NEW_VALUES, 1L, 1L,
                "testTag=test", false, streamEventMetadata);

        SourceInfo expected = new SourceInfo(connectorConfig, dataChangeEvent.getTableName(), recordTimestamp,
                commitTimestamp.toSqlTimestamp().toInstant(), readAtTimestamp, serverTransactionId, recordSequence,
                lowWatermark, numberRecordsInTransaction, "testTag=test", false,
                ValueCaptureType.OLD_AND_NEW_VALUES.name(), "testToken", 0, false, 1L);

        SourceInfo sourceInfo = sourceInfoFactory.getSourceInfo(0,
                new DataChangeEvent("token", commitTimestamp, "testId",
                        true, "1", "Table Name", rowType,
                        new ArrayList<>(), ModType.INSERT, ValueCaptureType.OLD_AND_NEW_VALUES, 1L,
                        1L, "testTag=test", false, streamEventMetadata));

        assertEquals(expected.getProjectId(), sourceInfo.getProjectId());
        assertEquals(expected.getInstanceId(), sourceInfo.getInstanceId());
        assertEquals(expected.getDatabaseId(), sourceInfo.getDatabaseId());
        assertEquals(expected.getChangeStreamName(), sourceInfo.getChangeStreamName());
        assertEquals(expected.getTableName(), sourceInfo.getTableName());
        assertEquals(expected.getRecordTimestamp(), sourceInfo.getRecordTimestamp());
        assertEquals(expected.getCommitTimestamp(), sourceInfo.getCommitTimestamp());
        assertEquals(expected.getServerTransactionId(), sourceInfo.getServerTransactionId());
        assertEquals(expected.getRecordSequence(), sourceInfo.getRecordSequence());
        assertEquals(expected.getLowWatermark(), sourceInfo.getLowWatermark());
        assertEquals(expected.getReadAtTimestamp(), sourceInfo.getReadAtTimestamp());
        assertEquals(expected.getNumberRecordsInTransaction(), sourceInfo.getNumberRecordsInTransaction());
    }

    @Test
    void testGetSourceInfoNewRow() throws InterruptedException {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        when(configuration.getString(anyString())).thenReturn("String");
        SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(configuration);
        SourceInfoFactory sourceInfoFactory = new SourceInfoFactory(
                connectorConfig, mock(LowWatermarkProvider.class));
        StreamEventMetadata streamEventMetadata = mock(StreamEventMetadata.class);
        when(streamEventMetadata.getRecordReadAt()).thenReturn(Timestamp.ofTimeMicroseconds(1L));
        ArrayList<Column> rowType = new ArrayList<>();

        Timestamp commitTimestamp = Timestamp.ofTimeMicroseconds(1L);
        Instant recordTimestamp = commitTimestamp.toSqlTimestamp().toInstant();
        Instant readAtTimestamp = commitTimestamp.toSqlTimestamp().toInstant();
        String serverTransactionId = "testId";
        Long recordSequence = 1L;
        Instant lowWatermark = null;
        Long numberRecordsInTransaction = 1L;

        DataChangeEvent dataChangeEvent = new DataChangeEvent("token", commitTimestamp,
                "testId", true,
                "1", "Table Name", rowType, new ArrayList<>(), ModType.INSERT,
                ValueCaptureType.NEW_ROW, 1L, 1L,
                "testTag=test", false, streamEventMetadata);

        SourceInfo expected = new SourceInfo(connectorConfig, dataChangeEvent.getTableName(), recordTimestamp,
                commitTimestamp.toSqlTimestamp().toInstant(), readAtTimestamp, serverTransactionId, recordSequence,
                lowWatermark, numberRecordsInTransaction, "testTag=test", false,
                ValueCaptureType.NEW_ROW.name(), "testToken", 0, false, 1L);

        SourceInfo sourceInfo = sourceInfoFactory.getSourceInfo(0,
                new DataChangeEvent("token", commitTimestamp, "testId",
                        true, "1", "Table Name", rowType,
                        new ArrayList<>(), ModType.INSERT, ValueCaptureType.NEW_ROW, 1L,
                        1L, "testTag=test", false, streamEventMetadata));

        assertEquals(expected.getValueCaptureType(), sourceInfo.getValueCaptureType());
        assertEquals(expected.getProjectId(), sourceInfo.getProjectId());
        assertEquals(expected.getInstanceId(), sourceInfo.getInstanceId());
        assertEquals(expected.getDatabaseId(), sourceInfo.getDatabaseId());
        assertEquals(expected.getChangeStreamName(), sourceInfo.getChangeStreamName());
        assertEquals(expected.getTableName(), sourceInfo.getTableName());
        assertEquals(expected.getRecordTimestamp(), sourceInfo.getRecordTimestamp());
        assertEquals(expected.getCommitTimestamp(), sourceInfo.getCommitTimestamp());
        assertEquals(expected.getServerTransactionId(), sourceInfo.getServerTransactionId());
        assertEquals(expected.getRecordSequence(), sourceInfo.getRecordSequence());
        assertEquals(expected.getLowWatermark(), sourceInfo.getLowWatermark());
        assertEquals(expected.getReadAtTimestamp(), sourceInfo.getReadAtTimestamp());
        assertEquals(expected.getNumberRecordsInTransaction(), sourceInfo.getNumberRecordsInTransaction());
    }

    @Test
    void testGetSourceInfoForLowWatermarkStamp() throws InterruptedException {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        SourceInfoFactory sourceInfoFactory = new SourceInfoFactory(
                new SpannerConnectorConfig(configuration), mock(LowWatermarkProvider.class));
        SourceInfo actualSourceInfoForLowWatermarkStamp = sourceInfoFactory
                .getSourceInfoForLowWatermarkStamp(TableId.getTableId("Table Name"));

        assertNull(actualSourceInfoForLowWatermarkStamp.database());
        assertNull(actualSourceInfoForLowWatermarkStamp.timestamp());
        assertNull(actualSourceInfoForLowWatermarkStamp.sequence());
        assertEquals("Table Name", actualSourceInfoForLowWatermarkStamp.getTableName());
        assertNull(actualSourceInfoForLowWatermarkStamp.getChangeStreamName());
        assertNull(actualSourceInfoForLowWatermarkStamp.getProjectId());
        assertNull(actualSourceInfoForLowWatermarkStamp.getReadAtTimestamp());
        assertNull(actualSourceInfoForLowWatermarkStamp.getInstanceId());
        assertNull(actualSourceInfoForLowWatermarkStamp.getCommitTimestamp());
        assertNull(actualSourceInfoForLowWatermarkStamp.getLowWatermark());
        assertNull(actualSourceInfoForLowWatermarkStamp.getServerTransactionId());
        assertNull(actualSourceInfoForLowWatermarkStamp.getNumberRecordsInTransaction());
    }
}
