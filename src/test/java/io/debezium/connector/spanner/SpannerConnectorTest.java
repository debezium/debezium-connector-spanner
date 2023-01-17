/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class SpannerConnectorTest {

    @Test
    void testVersion() {
        assertNotNull(new SpannerConnector().version());
    }

    @Test
    void testTaskClass() {
        Class<? extends Task> actualTaskClassResult = new SpannerConnector().taskClass();
        assertSame(SpannerConnectorTask.class, actualTaskClassResult);
    }

    @Test
    void testStart() {
        SpannerConnector spannerConnector = spy(new SpannerConnector());
        doNothing().when(spannerConnector).createInternalTopics(any());
        Map<String, String> props = new HashMap<>();
        props.put("name", "connector");
        spannerConnector.start(props);
        assertEquals(2, spannerConnector.getProps(1).size());
    }

    @Test
    void testStop() {
        SpannerConnector spannerConnector = spy(new SpannerConnector());
        doNothing().when(spannerConnector).createInternalTopics(any());
        Map<String, String> props = new HashMap<>();
        props.put("name", "connector");
        spannerConnector.start(props);
        spannerConnector.stop();
        assertThrows(NullPointerException.class, () -> spannerConnector.getProps(1));
    }

    @Test
    void testTaskConfigs() {
        SpannerConnector spannerConnector = spy(new SpannerConnector());
        doNothing().when(spannerConnector).createInternalTopics(any());
        Map<String, String> props = new HashMap<>();
        props.put("name", "connector");
        spannerConnector.start(props);
        List<Map<String, String>> maps = spannerConnector.taskConfigs(3);
        assertEquals(10, maps.size());
    }

    @Test
    @Disabled
    void testValidate() {
        SpannerConnector spannerConnector = new SpannerConnector();
        List<ConfigValue> configValuesResult = spannerConnector.validate(new HashMap<>()).configValues();
        assertEquals(45, configValuesResult.size());
        ConfigValue getResult = configValuesResult.get(43);
        assertTrue(getResult.visible());
        ConfigValue getResult1 = configValuesResult.get(3);
        assertTrue(getResult1.visible());
        assertNull(getResult1.value());
        List<Object> recommendedValuesResult = getResult1.recommendedValues();
        assertTrue(recommendedValuesResult.isEmpty());
        assertEquals("query.fetch.size", getResult1.name());
        List<String> errorMessagesResult = getResult1.errorMessages();
        assertEquals(recommendedValuesResult, errorMessagesResult);
        assertNull(getResult.value());
        List<Object> recommendedValuesResult1 = getResult.recommendedValues();
        assertEquals(errorMessagesResult, recommendedValuesResult1);
        assertEquals("gcp.spanner.database.id", getResult.name());
        List<String> errorMessagesResult1 = getResult.errorMessages();
        assertEquals(1, errorMessagesResult1.size());
        assertEquals("The 'gcp.spanner.database.id' value is invalid: The field is not specified",
                errorMessagesResult1.get(0));
        ConfigValue getResult2 = configValuesResult.get(42);
        assertNull(getResult2.value());
        List<Object> recommendedValuesResult2 = getResult2.recommendedValues();
        assertEquals(recommendedValuesResult1, recommendedValuesResult2);
        assertEquals("connector.spanner.rebalancing.commit.offsets.timeout", getResult2.name());
        List<String> errorMessagesResult2 = getResult2.errorMessages();
        assertEquals(recommendedValuesResult2, errorMessagesResult2);
        ConfigValue getResult3 = configValuesResult.get(2);
        assertNull(getResult3.value());
        List<Object> recommendedValuesResult3 = getResult3.recommendedValues();
        assertEquals(errorMessagesResult2, recommendedValuesResult3);
        assertEquals("max.queue.size", getResult3.name());
        List<String> errorMessagesResult3 = getResult3.errorMessages();
        assertEquals(recommendedValuesResult3, errorMessagesResult3);
        ConfigValue getResult4 = configValuesResult.get(44);
        assertNull(getResult4.value());
        List<Object> recommendedValuesResult4 = getResult4.recommendedValues();
        assertEquals(errorMessagesResult3, recommendedValuesResult4);
        assertEquals("connector.spanner.sync.delivery.timeout.ms", getResult4.name());
        List<String> errorMessagesResult4 = getResult4.errorMessages();
        assertEquals(recommendedValuesResult4, errorMessagesResult4);
        ConfigValue getResult5 = configValuesResult.get(0);
        assertNull(getResult5.value());
        List<Object> recommendedValuesResult5 = getResult5.recommendedValues();
        assertEquals(errorMessagesResult4, recommendedValuesResult5);
        assertEquals("gcp.spanner.credentials.path", getResult5.name());
        assertEquals(recommendedValuesResult5, getResult5.errorMessages());
        assertTrue(getResult4.visible());
        assertTrue(getResult5.visible());
        assertTrue(getResult2.visible());
        assertTrue(getResult3.visible());
    }

    @Test
    void testConfig() {
        ConfigDef actualConfigResult = new SpannerConnector().config();
        Map<String, ConfigDef.ConfigKey> configKeysResult = actualConfigResult.configKeys();
        assertEquals(52, configKeysResult.size());
        List<String> groupsResult = actualConfigResult.groups();
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
        assertEquals(3, getResult.orderInGroup);
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
        assertEquals(4, getResult4.orderInGroup);
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
        assertEquals(5, getResult5.orderInGroup);
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
}
