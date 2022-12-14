/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;

class SpannerConnectorConfigTest {

    @Test
    void testGetContextName() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertEquals("Spanner", new SpannerConnectorConfig(configuration).getContextName());
    }

    @Test
    void testGetConnectorName() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        assertNull(new SpannerConnectorConfig(configuration).getConnectorName());
    }

    @Test
    void testGetSourceInfoStructMaker() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((io.debezium.config.Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        Schema schemaResult = new SpannerConnectorConfig(configuration)
                .getSourceInfoStructMaker(CommonConnectorConfig.Version.V1)
                .schema();
        assertTrue(schemaResult instanceof ConnectSchema);
        assertNull(schemaResult.defaultValue());
        assertNull(schemaResult.version());
        assertEquals(Schema.Type.STRUCT, schemaResult.type());
        assertNull(schemaResult.parameters());
        assertEquals("com.google.spanner.connector.Source", schemaResult.name());
        assertFalse(schemaResult.isOptional());
        List<org.apache.kafka.connect.data.Field> fieldsResult = schemaResult.fields();
        assertEquals(23, fieldsResult.size());
        assertNull(schemaResult.doc());
        org.apache.kafka.connect.data.Field getResult = fieldsResult.get(0);
        assertTrue(getResult.schema() instanceof ConnectSchema);
        org.apache.kafka.connect.data.Field getResult1 = fieldsResult.get(1);
        assertTrue(getResult1.schema() instanceof ConnectSchema);
        org.apache.kafka.connect.data.Field getResult2 = fieldsResult.get(17);
        assertTrue(getResult2.schema() instanceof ConnectSchema);
        assertEquals(17, getResult2.index());
        assertEquals(0, getResult.index());
        assertEquals("version", getResult.name());
        assertEquals("system_transaction", getResult2.name());
        assertEquals(1, getResult1.index());
        assertEquals("connector", getResult1.name());
        org.apache.kafka.connect.data.Field getResult3 = fieldsResult.get(Short.SIZE);
        assertEquals(Short.SIZE, getResult3.index());
        assertEquals("transaction_tag", getResult3.name());
    }

    @Test
    void testKafkaProps() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");
        when(configuration.getString(anyString())).thenReturn("String");
        when(configuration.asProperties()).thenReturn(new Properties());
        SpannerConnectorConfig spannerConnectorConfig = new SpannerConnectorConfig(configuration);
        assertEquals(3, spannerConnectorConfig.kafkaProps(new HashMap<>()).size());
    }
}
