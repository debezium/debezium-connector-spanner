/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * Uses Kafka Admin Client to receive collection of partitions
 * for Kafka topic.
 */
public class KafkaPartitionInfoProvider {
    private final AdminClient adminClient;

    public KafkaPartitionInfoProvider(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public Collection<Integer> getPartitions(String topicName) throws ExecutionException, InterruptedException {

        DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topicName));

        KafkaFuture<TopicDescription> topicDescription = result.topicNameValues().get(topicName);
        try {
            return topicDescription.get().partitions().stream()
                    .map(TopicPartitionInfo::partition)
                    .collect(Collectors.toSet());
        }
        catch (ExecutionException ex) {
            if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                return Set.of();
            }
            throw ex;
        }
    }

}
