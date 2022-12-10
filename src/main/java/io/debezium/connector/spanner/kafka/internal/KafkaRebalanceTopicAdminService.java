/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;

import io.debezium.connector.spanner.SpannerConnectorConfig;

/**
 * Provides functionality to create and change Rebalance topic
 */
public class KafkaRebalanceTopicAdminService {
    private final AdminClient adminClient;

    private final String rebalancingTopic;
    private final int maxTasks;

    public KafkaRebalanceTopicAdminService(AdminClient adminClient, SpannerConnectorConfig config) {
        this.adminClient = adminClient;
        this.rebalancingTopic = config.rebalancingTopic();
        this.maxTasks = config.getMaxTasks();
    }

    public void createAdjustRebalanceTopic() {
        try {
            if (!topicExists()) {
                createTopic();
                return;
            }

            if (partitionCount() < maxTasks) {
                increasePartitions();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private boolean topicExists() throws ExecutionException, InterruptedException {
        Set<String> topics = adminClient.listTopics().names().get();
        return topics.contains(rebalancingTopic);
    }

    private void createTopic() throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(rebalancingTopic, Optional.of(maxTasks), Optional.empty());
        CreateTopicsResult result = adminClient.createTopics(List.of(newTopic));
        result.topicId(rebalancingTopic).get();
    }

    private void increasePartitions() {
        adminClient.createPartitions(Map.of(rebalancingTopic, NewPartitions.increaseTo(maxTasks)));
    }

    private int partitionCount() throws ExecutionException, InterruptedException {
        DescribeTopicsResult result = adminClient.describeTopics(List.of(rebalancingTopic));
        var description = result.topicNameValues().get(rebalancingTopic);
        return description.get().partitions().size();
    }
}
