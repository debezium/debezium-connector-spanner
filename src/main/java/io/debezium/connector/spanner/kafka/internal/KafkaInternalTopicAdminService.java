/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.TopicConfig;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.config.BaseSpannerConnectorConfig;
import io.debezium.connector.spanner.kafka.KafkaUtils;

/**
 * Provides functionality to create and change Rebalance and Sync topics
 */
public class KafkaInternalTopicAdminService {
    private final AdminClient adminClient;
    private final SpannerConnectorConfig config;

    public KafkaInternalTopicAdminService(AdminClient adminClient, SpannerConnectorConfig config) {
        this.adminClient = adminClient;
        this.config = config;
    }

    public void createAdjustRebalanceTopic() {
        try {
            String rebalancingTopic = config.rebalancingTopic();
            int maxTasks = config.getMaxTasks();
            Map<String, String> rebalancingTopicPassThroughProps = config.getConfig()
                    .subset(BaseSpannerConnectorConfig.CONNECTOR_SPANNER_REBALANCING_TOPIC_CONFIG_PREFIX, true).asMap();
            if (!topicExists(rebalancingTopic)) {
                createTopic(rebalancingTopic, Optional.of(maxTasks), rebalancingTopicPassThroughProps);
                return;
            }

            if (partitionCount(rebalancingTopic) < maxTasks) {
                increasePartitions(rebalancingTopic, maxTasks);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    public void createVerifySyncTopic() {
        try {
            String syncTopic = config.taskSyncTopic();
            if (!topicExists(syncTopic)) {
                Map<String, String> topicProps = new HashMap<>();
                topicProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, config.syncCleanupPolicy());
                topicProps.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(config.syncRetentionMs()));
                topicProps.put(TopicConfig.SEGMENT_MS_CONFIG, String.valueOf(config.syncSegmentMs()));
                topicProps.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, config.syncMinCleanableDirtyRatio());
                topicProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, config.syncTopicMaxMessageSize());

                Map<String, String> syncTopicPassThroughProps = config.getConfig()
                        .subset(BaseSpannerConnectorConfig.CONNECTOR_SPANNER_SYNC_TOPIC_CONFIG_PREFIX, true).asMap();
                if (!syncTopicPassThroughProps.isEmpty()) {
                    topicProps.putAll(syncTopicPassThroughProps);
                }

                createTopic(syncTopic, Optional.of(1), topicProps);
                return;
            }

            if (partitionCount(syncTopic) != 1) {
                throw new IllegalStateException("Sync topic must only contain 1 partition");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private boolean topicExists(String topic) throws ExecutionException, InterruptedException {
        return KafkaUtils.topicExists(adminClient, topic);
    }

    private void createTopic(String topic, Optional<Integer> numPartitions, Map<String, String> configs) throws ExecutionException, InterruptedException {
        KafkaUtils.createTopic(adminClient, topic, numPartitions, configs);
    }

    private void increasePartitions(String topic, int maxTasks) {
        adminClient.createPartitions(Map.of(topic, NewPartitions.increaseTo(maxTasks)));
    }

    private int partitionCount(String topic) throws ExecutionException, InterruptedException {
        DescribeTopicsResult result = adminClient.describeTopics(List.of(topic));
        var description = result.topicNameValues().get(topic);
        return description.get().partitions().size();
    }
}
