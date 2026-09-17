/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an integration test class as safe to run against a real Cloud Spanner instance
 * (i.e. when {@code -Dspanner.test.real=true} is passed on the command line).
 *
 * <p>All integration tests extending {@link AbstractSpannerConnectorIT} are emulator-only by
 * default: they only switch to a real Cloud Spanner backend if the test class explicitly opts in
 * with this annotation and overrides its connection/config fields accordingly (see
 * {@link RealSpannerTestSupport}). This way, accidentally running the full suite with
 * {@code -Dspanner.test.real=true} does not exercise destructive DDL/CRUD lifecycles against a real
 * GCP project for tests that haven't been reviewed for that. Tests that have been reviewed and are
 * meant to be run against real Spanner should opt in explicitly with this annotation.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RealSpannerCompatible {
}
