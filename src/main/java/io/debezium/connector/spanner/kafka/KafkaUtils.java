/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

public class KafkaUtils {
    private KafkaUtils() {
    }

    public static boolean topicExists(AdminClient adminClient, String topic) throws ExecutionException, InterruptedException {
        Set<String> topics = adminClient.listTopics().names().get();
        return topics.contains(topic);
    }

    public static void createTopic(AdminClient adminClient, String topic, int numPartitions, Map<String, String> configs)
            throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topic, Optional.of(numPartitions), Optional.empty()).configs(configs);
        CreateTopicsResult result = adminClient.createTopics(List.of(newTopic));
        result.topicId(topic).get();
    }
}
