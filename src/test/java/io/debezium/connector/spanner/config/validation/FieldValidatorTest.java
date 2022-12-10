/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.config.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

class FieldValidatorTest {

    private static Stream<Arguments> heartBeatIntervalProvider() {
        return Stream.of(
                Arguments.of(300001L, 1),
                Arguments.of(300000L, 0),
                Arguments.of(299999L, 0),
                Arguments.of(1001L, 0),
                Arguments.of(1000L, 0),
                Arguments.of(999L, 0),
                Arguments.of(99L, 1),
                Arguments.of(2000L, 0));
    }

    @ParameterizedTest
    @MethodSource("heartBeatIntervalProvider")
    void testIsCorrectHeartBeatInterval(Long interval, int expectedResult) {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getLong((Field) any())).thenReturn(interval);

        Field.ValidationOutput problems = mock(Field.ValidationOutput.class);

        int result = FieldValidator.isCorrectHeartBeatInterval(configuration, Field.create("Name"), problems);
        Assertions.assertEquals(expectedResult, result);
    }

    @Test
    void testIsBlank() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("");

        Field.ValidationOutput problems = mock(Field.ValidationOutput.class);

        assertEquals(1, FieldValidator.isNotBlank(configuration, Field.create("Name"), problems));
    }

    @Test
    void testIsNotBlank() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn("String");

        assertEquals(0, FieldValidator.isNotBlank(configuration, Field.create("Name"), null));
    }

    private static Stream<Arguments> pathProvider() {
        return Stream.of(
                Arguments.of(null, 0),
                Arguments.of("", 1),
                Arguments.of("path", 1),
                Arguments.of("/", 1));
    }

    @ParameterizedTest
    @MethodSource("pathProvider")
    void testIsCorrectPath(String value, int expected) {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn(value);

        Field.ValidationOutput problems = mock(Field.ValidationOutput.class);

        assertEquals(expected, FieldValidator.isCorrectPath(configuration, Field.create("Name"), problems));
    }

    private static Stream<Arguments> jsonProvider() {
        return Stream.of(
                Arguments.of(null, 0),
                Arguments.of("", 1),
                Arguments.of("42", 0),
                Arguments.of("[", 1),
                Arguments.of(Boolean.FALSE.toString(), 0),
                Arguments.of("{}", 0));
    }

    @ParameterizedTest
    @MethodSource("jsonProvider")
    void testIsCorrectJson(String value, int expected) {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn(value);

        Field.ValidationOutput problems = mock(Field.ValidationOutput.class);

        assertEquals(expected, FieldValidator.isCorrectJson(configuration, Field.create("Name"), problems));
    }

    private static Stream<Arguments> valueCaptureModeProvider() {
        return Stream.of(
                Arguments.of(null, 0),
                Arguments.of("OLD_AND_NEW_VALUES", 0),
                Arguments.of("NEW_VALUES", 1));
    }

    @ParameterizedTest
    @MethodSource("valueCaptureModeProvider")
    void testIsCorrectCaptureMode(String value, int expected) {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn(value);

        Field.ValidationOutput problems = mock(Field.ValidationOutput.class);

        assertEquals(expected, FieldValidator.isCorrectCaptureMode(configuration, Field.create("Name"), problems));
    }

    private static Stream<Arguments> dateTimeProvider() {
        return Stream.of(
                Arguments.of(null, 0),
                Arguments.of("", 1),
                Arguments.of("2022-10-17T19:15:48.622378", 1),
                Arguments.of("2022-10-18T17:17:39.257409Z", 0),
                Arguments.of("[]", 1));
    }

    @ParameterizedTest
    @MethodSource("dateTimeProvider")
    void testIsCorrectDateTime(String value, int expected) {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getString((Field) any())).thenReturn(value);

        Field.ValidationOutput problems = mock(Field.ValidationOutput.class);

        assertEquals(expected, FieldValidator.isCorrectDateTime(configuration, Field.create("Name"), problems));
    }

    @Test
    void testIsSpecified() {
        assertTrue(FieldValidator.isSpecified("42"));
        assertFalse(FieldValidator.isSpecified(""));
    }
}
