/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static java.util.stream.Collectors.toSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.KafkaFuture;

/**
 * Utility to retrieve information about Kafka Consumer Group
 */
public class KafkaConsumerAdminService {
    private final AdminClient adminClient;
    private final String consumerGroup;

    public KafkaConsumerAdminService(AdminClient adminClient, String consumerGroup) {
        this.adminClient = adminClient;
        this.consumerGroup = consumerGroup;
    }

    public Set<String> getActiveConsumerGroupMembers() {
        DescribeConsumerGroupsResult describeConsumerGroupsResult = adminClient.describeConsumerGroups(List.of(consumerGroup));
        Map<String, KafkaFuture<ConsumerGroupDescription>> kafkaFutureMap = describeConsumerGroupsResult.describedGroups();
        Set<Map.Entry<String, KafkaFuture<ConsumerGroupDescription>>> entries = kafkaFutureMap.entrySet();

        Set<String> result = new HashSet<>();
        for (var entry : entries) {
            KafkaFuture<ConsumerGroupDescription> value = entry.getValue();
            try {
                ConsumerGroupDescription desc = value.get();
                Set<String> consumerIds = desc.members()
                        .stream()
                        .map(MemberDescription::consumerId)
                        .collect(toSet());

                result.addAll(consumerIds);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }
}
