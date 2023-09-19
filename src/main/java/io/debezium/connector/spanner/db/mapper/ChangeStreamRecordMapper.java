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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;

import io.debezium.connector.spanner.db.dao.ChangeStreamResultSet;
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

    private final JsonFormat.Printer printer;
    private final JsonFormat.Parser parser;
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

    private final DatabaseClient databaseClient;

    public ChangeStreamRecordMapper(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;

        this.printer = JsonFormat.printer().preservingProtoFieldNames()
                .omittingInsignificantWhitespace();
        this.parser = JsonFormat.parser().ignoringUnknownFields();
    }

    public List<ChangeStreamEvent> toChangeStreamEvents(
                                                        Partition partition, ChangeStreamResultSet resultSet,
                                                        ChangeStreamResultSetMetadata resultSetMetadata) {
        if (this.isPostgres()) {
            // In PostgresQL, change stream records are returned as JsonB.
            return Collections.singletonList(
                    toStreamEventJson(partition, resultSet.getPgJsonb(0), resultSetMetadata));
        }
        // In GoogleSQL, change stream records are returned as an array of structs.
        return resultSet.getCurrentRowAsStruct().getStructList(0).stream()
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

        final Stream<ChildPartitionsEvent> childPartitionsEvents = row.getStructList(
                CHILD_PARTITIONS_RECORD_COLUMN).stream()
                .filter(this::isNonNullChildPartitionsRecord)
                .map(struct -> toChildPartitionsEvent(partition, struct, resultSetMetadata));

        return Stream.concat(
                Stream.concat(dataChangeEvents, heartbeatEvents), childPartitionsEvents);
    }

    ChangeStreamEvent toStreamEventJson(
                                        Partition partition, String row, ChangeStreamResultSetMetadata resultSetMetadata) {
        Value.Builder valueBuilder = Value.newBuilder();
        try {
            this.parser.merge(row, valueBuilder);
        }
        catch (InvalidProtocolBufferException exc) {
            throw new IllegalArgumentException("Failed to parse record into proto: " + row);
        }
        Value value = valueBuilder.build();
        if (isNonNullDataChangeRecordJson(value)) {
            return toDataChangeEventJson(partition, value, resultSetMetadata);
        }
        else if (isNonNullHeartbeatRecordJson(value)) {
            return toHeartbeatRecordJson(partition, value, resultSetMetadata);
        }
        else if (isNonNullChildPartitionsRecordJson(value)) {
            return toChildPartitionsRecordJson(partition, value, resultSetMetadata);
        }
        else {
            throw new IllegalArgumentException("Unknown change stream record type " + row);
        }
    }

    private HeartbeatEvent toHeartbeatRecordJson(
                                                 Partition partition, Value row, ChangeStreamResultSetMetadata resultSetMetadata) {
        Value heartBeatRecordValue = Optional.ofNullable(
                row.getStructValue().getFieldsMap().get(HEARTBEAT_RECORD_COLUMN))
                .orElseThrow(IllegalArgumentException::new);
        Map<String, Value> valueMap = heartBeatRecordValue.getStructValue().getFieldsMap();
        String heartbeatTimestamp = Optional.ofNullable(valueMap.get(TIMESTAMP_COLUMN))
                .orElseThrow(IllegalArgumentException::new)
                .getStringValue();

        return new HeartbeatEvent(
                Timestamp.parseTimestamp(heartbeatTimestamp),
                streamEventMetadataFrom(
                        partition, Timestamp.parseTimestamp(heartbeatTimestamp), resultSetMetadata));
    }

    private ChildPartitionsEvent toChildPartitionsRecordJson(
                                                             Partition partition, Value row, ChangeStreamResultSetMetadata resultSetMetadata) {
        Value childPartitionsRecordValue = Optional.ofNullable(
                row.getStructValue().getFieldsMap().get(CHILD_PARTITIONS_RECORD_COLUMN))
                .orElseThrow(IllegalArgumentException::new);
        Map<String, Value> valueMap = childPartitionsRecordValue.getStructValue().getFieldsMap();
        String startTimestamp = Optional.ofNullable(valueMap.get(START_TIMESTAMP_COLUMN))
                .orElseThrow(IllegalArgumentException::new)
                .getStringValue();

        return new ChildPartitionsEvent(
                Timestamp.parseTimestamp(startTimestamp),
                Optional.ofNullable(valueMap.get(RECORD_SEQUENCE_COLUMN))
                        .orElseThrow(IllegalArgumentException::new)
                        .getStringValue(),
                Optional.ofNullable(valueMap.get(CHILD_PARTITIONS_COLUMN))
                        .orElseThrow(IllegalArgumentException::new).getListValue().getValuesList().stream()
                        .map(value -> childPartitionJsonFrom(partition.getToken(), value))
                        .collect(Collectors.toList()),
                streamEventMetadataFrom(
                        partition, Timestamp.parseTimestamp(startTimestamp), resultSetMetadata));
    }

    private ChildPartition childPartitionJsonFrom(String partitionToken, Value row) {
        Map<String, Value> valueMap = row.getStructValue().getFieldsMap();
        final HashSet<String> parentTokens = Sets.newHashSet();
        for (Value parentToken : Optional.ofNullable(valueMap.get(PARENT_PARTITION_TOKENS_COLUMN))
                .orElseThrow(IllegalArgumentException::new)
                .getListValue()
                .getValuesList()) {
            parentTokens.add(parentToken.getStringValue());
        }
        if (InitialPartition.isInitialPartition(partitionToken)) {
            parentTokens.add(partitionToken);
        }
        return new ChildPartition(
                Optional.ofNullable(valueMap.get(TOKEN_COLUMN))
                        .orElseThrow(IllegalArgumentException::new)
                        .getStringValue(),
                parentTokens);
    }

    private boolean isNonNullDataChangeRecordJson(Value row) {
        return row.getStructValue().getFieldsMap().containsKey(DATA_CHANGE_RECORD_COLUMN);
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

    private boolean isNonNullHeartbeatRecordJson(Value row) {
        return row.getStructValue().getFieldsMap().containsKey(HEARTBEAT_RECORD_COLUMN);
    }

    private boolean isNonNullChildPartitionsRecordJson(Value row) {
        return row.getStructValue().getFieldsMap().containsKey(CHILD_PARTITIONS_RECORD_COLUMN);
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
                modTypeFrom(row.getString(MOD_TYPE_COLUMN)),
                valueCaptureTypeFrom(row.getString(VALUE_CAPTURE_TYPE_COLUMN)),
                row.getLong(NUMBER_OF_RECORDS_IN_TRANSACTION_COLUMN),
                row.getLong(NUMBER_OF_PARTITIONS_IN_TRANSACTION_COLUMN),
                row.getString(TRANSACTION_TAG),
                row.getBoolean(SYSTEM_TRANSACTION),
                streamEventMetadataFrom(partition, commitTimestamp, resultSetMetadata));
    }

    DataChangeEvent toDataChangeEventJson(Partition partition, Value row,
                                          ChangeStreamResultSetMetadata resultSetMetadata) {
        Value dataChangeRecordValue = Optional.ofNullable(
                row.getStructValue().getFieldsMap().get(DATA_CHANGE_RECORD_COLUMN))
                .orElseThrow(IllegalArgumentException::new);
        Map<String, Value> valueMap = dataChangeRecordValue.getStructValue().getFieldsMap();
        final String commitTimestamp = Optional.ofNullable(valueMap.get(COMMIT_TIMESTAMP_COLUMN))
                .orElseThrow(IllegalArgumentException::new)
                .getStringValue();
        AtomicInteger modIndex = new AtomicInteger();
        List<Mod> mods = Optional.ofNullable(valueMap.get(MODS_COLUMN))
                .orElseThrow(IllegalArgumentException::new)
                .getListValue().getValuesList().stream()
                .map(mod -> {
                    modIndex.getAndIncrement();
                    return modJsonFrom(mod, modIndex.get());
                })
                .collect(Collectors.toList());
        return new DataChangeEvent(
                partition.getToken(),
                Timestamp.parseTimestamp(commitTimestamp),
                Optional.ofNullable(valueMap.get(SERVER_TRANSACTION_ID_COLUMN))
                        .orElseThrow(IllegalArgumentException::new)
                        .getStringValue(),
                Optional.ofNullable(valueMap.get(IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION_COLUMN))
                        .orElseThrow(IllegalArgumentException::new)
                        .getBoolValue(),
                Optional.ofNullable(valueMap.get(RECORD_SEQUENCE_COLUMN))
                        .orElseThrow(IllegalArgumentException::new)
                        .getStringValue(),
                Optional.ofNullable(valueMap.get(TABLE_NAME_COLUMN))
                        .orElseThrow(IllegalArgumentException::new)
                        .getStringValue(),
                Optional.ofNullable(valueMap.get(COLUMN_TYPES_COLUMN))
                        .orElseThrow(IllegalArgumentException::new).getListValue().getValuesList().stream()
                        .map(this::columnTypeJsonFrom)
                        .collect(Collectors.toList()),
                mods,
                modTypeFrom(
                        Optional.ofNullable(valueMap.get(MOD_TYPE_COLUMN))
                                .orElseThrow(IllegalArgumentException::new)
                                .getStringValue()),
                valueCaptureTypeFrom(
                        Optional.ofNullable(valueMap.get(VALUE_CAPTURE_TYPE_COLUMN))
                                .orElseThrow(IllegalArgumentException::new)
                                .getStringValue()),
                (long) Optional.ofNullable(valueMap.get(NUMBER_OF_RECORDS_IN_TRANSACTION_COLUMN))
                        .orElseThrow(IllegalArgumentException::new)
                        .getNumberValue(),
                (long) Optional.ofNullable(valueMap.get(NUMBER_OF_PARTITIONS_IN_TRANSACTION_COLUMN))
                        .orElseThrow(IllegalArgumentException::new)
                        .getNumberValue(),
                Optional.ofNullable(valueMap.get(TRANSACTION_TAG))
                        .orElseThrow(IllegalArgumentException::new)
                        .getStringValue(),
                Optional.ofNullable(valueMap.get(SYSTEM_TRANSACTION))
                        .orElseThrow(IllegalArgumentException::new)
                        .getBoolValue(),
                streamEventMetadataFrom(
                        partition, Timestamp.parseTimestamp(commitTimestamp), resultSetMetadata));
    }

    private Column columnTypeJsonFrom(Value row) {
        Map<String, Value> valueMap = row.getStructValue().getFieldsMap();
        try {
            String type = this.printer.print(
                    Optional.ofNullable(valueMap.get(TYPE_COLUMN))
                            .orElseThrow(IllegalArgumentException::new));
            final ColumnType columnType = ColumnTypeParser.parse(type);
            return new Column(
                    Optional.ofNullable(valueMap.get(NAME_COLUMN))
                            .orElseThrow(IllegalArgumentException::new)
                            .getStringValue(),
                    columnType,
                    Optional.ofNullable(valueMap.get(IS_PRIMARY_KEY_COLUMN))
                            .orElseThrow(IllegalArgumentException::new)
                            .getBoolValue(),
                    (long) Optional.ofNullable(valueMap.get(ORDINAL_POSITION_COLUMN))
                            .orElseThrow(IllegalArgumentException::new)
                            .getNumberValue(),
                    null);
        }
        catch (InvalidProtocolBufferException exc) {
            throw new IllegalArgumentException("Failed to print type: " + row);
        }
    }

    private Mod modJsonFrom(Value row, int modNumber) {
        try {
            Map<String, Value> valueMap = row.getStructValue().getFieldsMap();
            final String keys = this.printer.print(
                    Optional.ofNullable(valueMap.get(KEYS_COLUMN))
                            .orElseThrow(IllegalArgumentException::new));

            final String oldValues = !valueMap.containsKey("old_values")
                    ? null
                    : this.printer.print(
                            Optional.ofNullable(valueMap.get(OLD_VALUES_COLUMN))
                                    .orElseThrow(IllegalArgumentException::new));
            final String newValues = !valueMap.containsKey("new_values")
                    ? null
                    : this.printer.print(
                            Optional.ofNullable(valueMap.get(NEW_VALUES_COLUMN))
                                    .orElseThrow(IllegalArgumentException::new));
            return new Mod(modNumber, MapperUtils.getJsonNode(keys),
                    MapperUtils.getJsonNode(oldValues), MapperUtils.getJsonNode(newValues));
        }
        catch (InvalidProtocolBufferException exc) {
            throw new IllegalArgumentException("Failed to print mod: " + row);
        }
    }

    private ValueCaptureType valueCaptureTypeFrom(String name) {
        try {
            return ValueCaptureType.valueOf(name);
        }
        catch (IllegalArgumentException e) {
            // This is not logged to prevent flooding users with messages
            return ValueCaptureType.UNKNOWN;
        }
    }

    private ModType modTypeFrom(String name) {
        try {
            return ModType.valueOf(name);
        }
        catch (IllegalArgumentException e) {
            // This is not logged to prevent flooding users with messages
            return ModType.UNKNOWN;
        }
    }

    @VisibleForTesting
    HeartbeatEvent toHeartbeatEvent(Partition partition, Struct row,
                                    ChangeStreamResultSetMetadata resultSetMetadata) {
        final Timestamp timestamp = row.getTimestamp(TIMESTAMP_COLUMN);

        return new HeartbeatEvent(timestamp,
                streamEventMetadataFrom(partition, timestamp, resultSetMetadata));
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

    private boolean isPostgres() {
        return this.databaseClient.getDialect() == Dialect.POSTGRESQL;
    }

}
