/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.spanner.SpannerConnectorConfig;

class BaseSpannerConnectorConfigTest {
    @Test
    void testConfigDef() {
        ConfigDef actualConfigDefResult = BaseSpannerConnectorConfig.configDef();
        Map<String, ConfigDef.ConfigKey> configKeysResult = actualConfigDefResult.configKeys();
        assertEquals(55, configKeysResult.size());
        List<String> groupsResult = actualConfigDefResult.groups();
        assertEquals(3, groupsResult.size());
        assertEquals("Spanner", groupsResult.get(0));
        assertEquals("Connector", groupsResult.get(1));
        assertEquals("Events", groupsResult.get(2));
        ConfigDef.ConfigKey getResult = configKeysResult.get("gcp.spanner.change.stream");
        assertEquals(ConfigDef.Width.MEDIUM, getResult.width);
        ConfigDef.ConfigKey getResult1 = configKeysResult.get("gcp.spanner.instance.id");
        assertEquals(ConfigDef.Width.SHORT, getResult1.width);
        assertNull(getResult1.validator);
        assertEquals(ConfigDef.Type.STRING, getResult1.type);
        assertNull(getResult1.recommender);
        assertEquals(1, getResult1.orderInGroup);
        assertEquals("gcp.spanner.instance.id", getResult1.name);
        assertFalse(getResult1.internalConfig);
        assertEquals(ConfigDef.Importance.HIGH, getResult1.importance);
        assertEquals("Connector", getResult1.group);
        assertEquals("Spanner instance id", getResult1.documentation);
        assertEquals("InstanceId", getResult1.displayName);
        assertTrue(getResult1.dependents.isEmpty());
        assertNull(getResult1.defaultValue);
        assertNull(getResult.validator);
        assertEquals(ConfigDef.Type.STRING, getResult.type);
        assertNull(getResult.recommender);
        assertEquals(4, getResult.orderInGroup);
        assertEquals("gcp.spanner.change.stream", getResult.name);
        assertFalse(getResult.internalConfig);
        assertEquals(ConfigDef.Importance.HIGH, getResult.importance);
        assertEquals("Connector", getResult.group);
        assertEquals("Spanner change stream name", getResult.documentation);
        assertEquals("Change stream name", getResult.displayName);
        assertNull(getResult.defaultValue);
        ConfigDef.ConfigKey getResult2 = configKeysResult.get("gcp.spanner.database.id");
        assertNull(getResult2.validator);
        assertEquals(ConfigDef.Type.STRING, getResult2.type);
        assertNull(getResult2.recommender);
        assertEquals(2, getResult2.orderInGroup);
        assertEquals("gcp.spanner.database.id", getResult2.name);
        assertFalse(getResult2.internalConfig);
        assertEquals(ConfigDef.Importance.HIGH, getResult2.importance);
        assertEquals("Connector", getResult2.group);
        assertEquals("Spanner database id", getResult2.documentation);
        assertEquals("DatabaseId", getResult2.displayName);
        assertNull(getResult2.defaultValue);
        ConfigDef.ConfigKey getResult3 = configKeysResult.get("gcp.spanner.project.id");
        assertNull(getResult3.validator);
        assertEquals(ConfigDef.Type.STRING, getResult3.type);
        assertNull(getResult3.recommender);
        assertEquals(1, getResult3.orderInGroup);
        assertEquals("gcp.spanner.project.id", getResult3.name);
        assertFalse(getResult3.internalConfig);
        assertEquals(ConfigDef.Importance.HIGH, getResult3.importance);
        assertEquals("Spanner", getResult3.group);
        assertEquals("Spanner project id", getResult3.documentation);
        assertEquals("ProjectId", getResult3.displayName);
        assertNull(getResult3.defaultValue);
        ConfigDef.ConfigKey getResult4 = configKeysResult.get("gcp.spanner.start.time");
        assertNull(getResult4.validator);
        assertEquals(ConfigDef.Type.STRING, getResult4.type);
        assertNull(getResult4.recommender);
        assertEquals(5, getResult4.orderInGroup);
        assertEquals("gcp.spanner.start.time", getResult4.name);
        assertFalse(getResult4.internalConfig);
        assertEquals(ConfigDef.Importance.MEDIUM, getResult4.importance);
        assertEquals("Connector", getResult4.group);
        assertEquals("Start change stream time", getResult4.documentation);
        assertEquals("Start time", getResult4.displayName);
        assertNull(getResult4.defaultValue);
        ConfigDef.ConfigKey getResult5 = configKeysResult.get("gcp.spanner.end.time");
        assertNull(getResult5.validator);
        assertEquals(ConfigDef.Type.STRING, getResult5.type);
        assertNull(getResult5.recommender);
        assertEquals(6, getResult5.orderInGroup);
        assertEquals("gcp.spanner.end.time", getResult5.name);
        assertFalse(getResult5.internalConfig);
        assertEquals(ConfigDef.Importance.MEDIUM, getResult5.importance);
        assertEquals("Connector", getResult5.group);
        assertEquals("End change stream time", getResult5.documentation);
        assertEquals("End time", getResult5.displayName);
        assertNull(getResult5.defaultValue);
        assertEquals(ConfigDef.Width.SHORT, getResult4.width);
        assertEquals(ConfigDef.Width.SHORT, getResult5.width);
        assertEquals(ConfigDef.Width.SHORT, getResult2.width);
        assertEquals(ConfigDef.Width.SHORT, getResult3.width);
    }

    @Test
    void testIsSchemaChangesHistoryEnabled() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertFalse(new SpannerConnectorConfig(configuration).isSchemaChangesHistoryEnabled());
    }

    @Test
    void testIsSchemaCommentsHistoryEnabled() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertFalse(new SpannerConnectorConfig(configuration).isSchemaCommentsHistoryEnabled());
    }

    @Test
    void testGetHeartbeatInterval() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        Duration interval = new SpannerConnectorConfig(configuration).getHeartbeatInterval();
        assertEquals(Duration.of(300000L, ChronoUnit.MILLIS), interval);
    }

    @Test
    void testGetLowWatermarkStampInterval() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        Duration interval = new SpannerConnectorConfig(configuration).getLowWatermarkStampInterval();
        assertEquals(Duration.of(10000L, ChronoUnit.MILLIS), interval);
    }

    @Test
    void testShouldProvideTransactionMetadata() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertFalse(new SpannerConnectorConfig(configuration).shouldProvideTransactionMetadata());
    }

    @Test
    void testProjectId() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertNull(new SpannerConnectorConfig(configuration).projectId());
    }

    @Test
    void testInstanceId() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertNull(new SpannerConnectorConfig(configuration).instanceId());
    }

    @Test
    void testDatabaseId() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertNull(new SpannerConnectorConfig(configuration).databaseId());
    }

    @Test
    void testChangeStreamName() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertNull(new SpannerConnectorConfig(configuration).changeStreamName());
    }
    //
    // @Test
    // void testStartTime() {
    // Configuration configuration = mock(Configuration.class);
    // when(configuration.getString((Field) any())).thenReturn("String");
    // when(configuration.asProperties()).thenReturn(new Properties());
    // Timestamp timestamp = new SpannerConnectorConfig(configuration).startTime();
    // assertTrue(Timestamp.now().getNanos() >= timestamp.getNanos());
    // }

    @Test
    void testEndTime() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertNull(new SpannerConnectorConfig(configuration).endTime());
    }

    @Test
    void testQueueCapacity() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals(10000, new SpannerConnectorConfig(configuration).queueCapacity());
    }

    @Test
    void testGcpSpannerCredentialsJson() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertNull(new SpannerConnectorConfig(configuration).gcpSpannerCredentialsJson());
    }

    @Test
    void testGcpSpannerCredentialsPath() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertNull(new SpannerConnectorConfig(configuration).gcpSpannerCredentialsPath());
    }

    @Test
    void testTableExcludeList() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertNull(new SpannerConnectorConfig(configuration).tableExcludeList());
    }

    @Test
    void testTableIncludeList() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertNull(new SpannerConnectorConfig(configuration).tableIncludeList());
    }

    @Test
    void testBootStrapServer() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals("localhost:9092", new SpannerConnectorConfig(configuration).bootStrapServer());
    }

    @Test
    void testRebalancingTopic() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals("_rebalancing_topic_spanner_connector_null",
                new SpannerConnectorConfig(configuration).rebalancingTopic());
    }

    @Test
    void testRebalancingPollDuration() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals(5000, new SpannerConnectorConfig(configuration).rebalancingPollDuration());
    }

    @Test
    void testRebalancingCommitOffsetsTimeout() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals(5000, new SpannerConnectorConfig(configuration).rebalancingCommitOffsetsTimeout());
    }

    @Test
    void testSyncPollDuration() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals(500, new SpannerConnectorConfig(configuration).syncPollDuration());
    }

    @Test
    void testSyncRequestTimeout() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals(5000, new SpannerConnectorConfig(configuration).syncRequestTimeout());
    }

    @Test
    void testSyncDeliveryTimeout() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals(15000, new SpannerConnectorConfig(configuration).syncDeliveryTimeout());
    }

    @Test
    void testTaskSyncTopic() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals("_sync_topic_spanner_connector_null", new SpannerConnectorConfig(configuration).taskSyncTopic());
    }

    @Test
    void testGetMaxTasks() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals(10, new SpannerConnectorConfig(configuration).getMaxTasks());
    }

    @Test
    void testGetMinTasks() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals(2, new SpannerConnectorConfig(configuration).getMinTasks());
    }

    @Test
    void testGetDesiredPartitionsTasks() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals(2, new SpannerConnectorConfig(configuration).getDesiredPartitionsTasks());
    }

    @Test
    void testIsLowWatermarkEnabled() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertFalse(new SpannerConnectorConfig(configuration).isLowWatermarkEnabled());
    }

    @Test
    void testGetLowWatermarkUpdatePeriodMs() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals(1000L, new SpannerConnectorConfig(configuration).getLowWatermarkUpdatePeriodMs());
    }
}
