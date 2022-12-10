/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.mapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.common.annotations.VisibleForTesting;

import io.debezium.connector.spanner.db.dao.ChangeStreamResultSetMetadata;
import io.debezium.connector.spanner.db.mapper.parser.ColumnTypeParser;
import io.debezium.connector.spanner.db.model.ChildPartition;
import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.Mod;
import io.debezium.connector.spanner.db.model.ModType;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.model.StreamEventMetadata;
import io.debezium.connector.spanner.db.model.ValueCaptureType;
import io.debezium.connector.spanner.db.model.event.ChangeStreamEvent;
import io.debezium.connector.spanner.db.model.event.ChildPartitionsEvent;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;
import io.debezium.connector.spanner.db.model.schema.Column;
import io.debezium.connector.spanner.db.model.schema.ColumnType;

/**
 * Maps Change Stream events from the raw format into specific DTOs
 */

public class ChangeStreamRecordMapper {

    private static final String DATA_CHANGE_RECORD_COLUMN = "data_change_record";
    private static final String HEARTBEAT_RECORD_COLUMN = "heartbeat_record";
    private static final String CHILD_PARTITIONS_RECORD_COLUMN = "child_partitions_record";

    private static final String COMMIT_TIMESTAMP_COLUMN = "commit_timestamp";
    private static final String SERVER_TRANSACTION_ID_COLUMN = "server_transaction_id";
    private static final String IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION_COLUMN = "is_last_record_in_transaction_in_partition";
    private static final String RECORD_SEQUENCE_COLUMN = "record_sequence";
    private static final String TABLE_NAME_COLUMN = "table_name";
    private static final String COLUMN_TYPES_COLUMN = "column_types";
    private static final String MODS_COLUMN = "mods";
    private static final String MOD_TYPE_COLUMN = "mod_type";
    private static final String VALUE_CAPTURE_TYPE_COLUMN = "value_capture_type";
    private static final String NUMBER_OF_RECORDS_IN_TRANSACTION_COLUMN = "number_of_records_in_transaction";
    private static final String NUMBER_OF_PARTITIONS_IN_TRANSACTION_COLUMN = "number_of_partitions_in_transaction";
    private static final String NAME_COLUMN = "name";
    private static final String TYPE_COLUMN = "type";
    private static final String IS_PRIMARY_KEY_COLUMN = "is_primary_key";
    private static final String ORDINAL_POSITION_COLUMN = "ordinal_position";
    private static final String KEYS_COLUMN = "keys";
    private static final String OLD_VALUES_COLUMN = "old_values";
    private static final String NEW_VALUES_COLUMN = "new_values";

    private static final String TIMESTAMP_COLUMN = "timestamp";

    private static final String START_TIMESTAMP_COLUMN = "start_timestamp";
    private static final String CHILD_PARTITIONS_COLUMN = "child_partitions";
    private static final String PARENT_PARTITION_TOKENS_COLUMN = "parent_partition_tokens";
    private static final String TOKEN_COLUMN = "token";

    private static final String TRANSACTION_TAG = "transaction_tag";

    private static final String SYSTEM_TRANSACTION = "is_system_transaction";

    public List<ChangeStreamEvent> toChangeStreamEvents(
                                                        Partition partition, Struct row, ChangeStreamResultSetMetadata resultSetMetadata) {
        return row.getStructList(0).stream()
                .flatMap(struct -> toStreamEvent(partition, struct, resultSetMetadata))
                .collect(Collectors.toList());
    }

    Stream<ChangeStreamEvent> toStreamEvent(Partition partition, Struct row,
                                            ChangeStreamResultSetMetadata resultSetMetadata) {
        final Stream<DataChangeEvent> dataChangeEvents = row.getStructList(DATA_CHANGE_RECORD_COLUMN).stream()
                .filter(this::isNonNullDataChangeRecord)
                .map(struct -> toDataChangeEvent(partition, struct, resultSetMetadata));

        final Stream<HeartbeatEvent> heartbeatEvents = row.getStructList(HEARTBEAT_RECORD_COLUMN).stream()
                .filter(this::isNonNullHeartbeatRecord)
                .map(struct -> toHeartbeatEvent(partition, struct, resultSetMetadata));

        final Stream<ChildPartitionsEvent> childPartitionsEvents = row.getStructList(CHILD_PARTITIONS_RECORD_COLUMN).stream()
                .filter(this::isNonNullChildPartitionsRecord)
                .map(struct -> toChildPartitionsEvent(partition, struct, resultSetMetadata));

        return Stream.concat(
                Stream.concat(dataChangeEvents, heartbeatEvents), childPartitionsEvents);
    }

    boolean isNonNullDataChangeRecord(Struct row) {
        return !row.isNull(COMMIT_TIMESTAMP_COLUMN);
    }

    boolean isNonNullHeartbeatRecord(Struct row) {
        return !row.isNull(TIMESTAMP_COLUMN);
    }

    boolean isNonNullChildPartitionsRecord(Struct row) {
        return !row.isNull(START_TIMESTAMP_COLUMN);
    }

    DataChangeEvent toDataChangeEvent(Partition partition, Struct row,
                                      ChangeStreamResultSetMetadata resultSetMetadata) {
        final Timestamp commitTimestamp = row.getTimestamp(COMMIT_TIMESTAMP_COLUMN);
        return new DataChangeEvent(
                partition.getToken(),
                commitTimestamp,
                row.getString(SERVER_TRANSACTION_ID_COLUMN),
                row.getBoolean(IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION_COLUMN),
                row.getString(RECORD_SEQUENCE_COLUMN),
                row.getString(TABLE_NAME_COLUMN),
                row.getStructList(COLUMN_TYPES_COLUMN).stream()
                        .map(this::columnTypeFrom)
                        .collect(Collectors.toList()),
                modListFrom(row.getStructList(MODS_COLUMN)),
                ModType.valueOf(row.getString(MOD_TYPE_COLUMN)),
                ValueCaptureType.valueOf(row.getString(VALUE_CAPTURE_TYPE_COLUMN)),
                row.getLong(NUMBER_OF_RECORDS_IN_TRANSACTION_COLUMN),
                row.getLong(NUMBER_OF_PARTITIONS_IN_TRANSACTION_COLUMN),
                row.getString(TRANSACTION_TAG),
                row.getBoolean(SYSTEM_TRANSACTION),
                streamEventMetadataFrom(partition, commitTimestamp, resultSetMetadata));
    }

    @VisibleForTesting
    HeartbeatEvent toHeartbeatEvent(Partition partition, Struct row,
                                    ChangeStreamResultSetMetadata resultSetMetadata) {
        final Timestamp timestamp = row.getTimestamp(TIMESTAMP_COLUMN);

        return new HeartbeatEvent(timestamp, streamEventMetadataFrom(partition, timestamp, resultSetMetadata));
    }

    @VisibleForTesting
    ChildPartitionsEvent toChildPartitionsEvent(Partition partition, Struct row,
                                                ChangeStreamResultSetMetadata resultSetMetadata) {
        final Timestamp startTimestamp = row.getTimestamp(START_TIMESTAMP_COLUMN);

        return new ChildPartitionsEvent(
                startTimestamp,
                row.getString(RECORD_SEQUENCE_COLUMN),
                row.getStructList(CHILD_PARTITIONS_COLUMN).stream()
                        .map(struct -> childPartitionFrom(partition.getToken(), struct))
                        .collect(Collectors.toList()),
                streamEventMetadataFrom(partition, startTimestamp, resultSetMetadata));
    }

    @VisibleForTesting
    Column columnTypeFrom(Struct struct) {
        final String type = getJsonString(struct, TYPE_COLUMN);
        final ColumnType columnType = ColumnTypeParser.parse(type);
        return new Column(
                struct.getString(NAME_COLUMN),
                columnType,
                struct.getBoolean(IS_PRIMARY_KEY_COLUMN),
                struct.getLong(ORDINAL_POSITION_COLUMN),
                null);
    }

    List<Mod> modListFrom(List<Struct> list) {
        List<Mod> mods = new ArrayList<>(list.size());
        for (Struct struct : list) {
            mods.add(this.modFrom(mods.size(), struct));
        }
        return mods;
    }

    @VisibleForTesting
    Mod modFrom(int modNumber, Struct struct) {
        final String keys = getJsonString(struct, KEYS_COLUMN);
        final String oldValues = struct.isNull(OLD_VALUES_COLUMN) ? null : getJsonString(struct, OLD_VALUES_COLUMN);
        final String newValues = struct.isNull(NEW_VALUES_COLUMN) ? null : getJsonString(struct, NEW_VALUES_COLUMN);
        return new Mod(modNumber, MapperUtils.getJsonNode(keys), MapperUtils.getJsonNode(oldValues), MapperUtils.getJsonNode(newValues));
    }

    @VisibleForTesting
    ChildPartition childPartitionFrom(String partitionToken, Struct struct) {
        final Set<String> parentTokens = new HashSet<>(struct.getStringList(PARENT_PARTITION_TOKENS_COLUMN));
        if (InitialPartition.isInitialPartition(partitionToken)) {
            parentTokens.add(partitionToken);
        }
        return new ChildPartition(struct.getString(TOKEN_COLUMN), Collections.unmodifiableSet(parentTokens));
    }

    @VisibleForTesting
    StreamEventMetadata streamEventMetadataFrom(
                                                Partition partition,
                                                Timestamp recordTimestamp,
                                                ChangeStreamResultSetMetadata resultSetMetadata) {
        return StreamEventMetadata.newBuilder()
                .withRecordTimestamp(recordTimestamp)
                .withPartitionToken(partition.getToken())
                .withPartitionStartTimestamp(partition.getStartTimestamp())
                .withPartitionEndTimestamp(partition.getEndTimestamp())
                .withQueryStartedAt(resultSetMetadata.getQueryStartedAt())
                .withRecordStreamStartedAt(resultSetMetadata.getRecordStreamStartedAt())
                .withRecordStreamEndedAt(resultSetMetadata.getRecordStreamEndedAt())
                .withRecordReadAt(resultSetMetadata.getRecordReadAt())
                .withTotalStreamTimeMillis(resultSetMetadata.getTotalStreamDuration().getMillis())
                .withNumberOfRecordsRead(resultSetMetadata.getNumberOfRecordsRead())
                .build();
    }

    @VisibleForTesting
    String getJsonString(Struct struct, String columnName) {
        if (struct.getColumnType(columnName).equals(Type.json())) {
            return struct.getJson(columnName);
        }
        else if (struct.getColumnType(columnName).equals(Type.string())) {
            return struct.getString(columnName);
        }
        else {
            throw new IllegalArgumentException("Can not extract string from value " + columnName);
        }
    }

}
