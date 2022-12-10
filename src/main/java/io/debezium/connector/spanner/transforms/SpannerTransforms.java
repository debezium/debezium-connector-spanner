/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.transforms;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.transforms.SmtManager;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Spanner Transform
 *
 * @author Zhylenko P
 */
public class SpannerTransforms<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerTransforms.class);
    private SchemaNameAdjuster schemaNameAdjuster;
    private Pattern topicRegex;
    private String topicReplacement;
    private Pattern keyFieldRegex;
    private boolean keyEnforceUniqueness;
    private String keyFieldReplacement;
    private String keyFieldName;
    // private final Cache<Schema, Schema> keySchemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    // private final Cache<Schema, Schema> envelopeSchemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    // private final Cache<String, String> keyRegexReplaceCache = new SynchronizedCache<>(new LRUCache<String, String>(16));
    // private final Cache<String, String> topicRegexReplaceCache = new SynchronizedCache<>(new LRUCache<String, String>(16));
    private SmtManager<R> smtManager;

    @Override
    public R apply(R record) {
        final String oldTopic = record.topic();
        final String newTopic = oldTopic.toUpperCase();

        return record.newRecord(
                newTopic,
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp());
    }

    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();
        // Field.group(
        // config,
        // null,
        // TOPIC_REGEX,
        // TOPIC_REPLACEMENT,
        // KEY_ENFORCE_UNIQUENESS,
        // KEY_FIELD_REGEX,
        // KEY_FIELD_REPLACEMENT);
        return config;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Configuration config = Configuration.from(configs);
        // final Field.Set configFields = Field.setOf(
        // TOPIC_REGEX,
        // TOPIC_REPLACEMENT,
        // KEY_ENFORCE_UNIQUENESS,
        // KEY_FIELD_REGEX,
        // KEY_FIELD_REPLACEMENT,
        // SCHEMA_NAME_ADJUSTMENT_MODE);
        //
        // if (!config.validateAndRecord(configFields, LOGGER::error)) {
        // throw new ConnectException("Unable to validate config.");
        // }
        //
        // topicRegex = Pattern.compile(config.getString(TOPIC_REGEX));
        // topicReplacement = config.getString(TOPIC_REPLACEMENT);
        //
        // String keyFieldRegexString = config.getString(KEY_FIELD_REGEX);
        // if (keyFieldRegexString != null) {
        // keyFieldRegexString = keyFieldRegexString.trim();
        // }
        // if (keyFieldRegexString != null && !keyFieldRegexString.isEmpty()) {
        // keyFieldRegex = Pattern.compile(config.getString(KEY_FIELD_REGEX));
        // keyFieldReplacement = config.getString(KEY_FIELD_REPLACEMENT);
        // }
        // keyFieldName = config.getString(KEY_FIELD_NAME);
        // keyEnforceUniqueness = config.getBoolean(KEY_ENFORCE_UNIQUENESS);
        //
        // smtManager = new SmtManager<>(config);
        //
        // schemaNameAdjuster = CommonConnectorConfig.SchemaNameAdjustmentMode.parse(config.getString(SCHEMA_NAME_ADJUSTMENT_MODE))
        // .createAdjuster();
    }
}
