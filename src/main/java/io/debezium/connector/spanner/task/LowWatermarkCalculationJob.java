/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.function.Consumer;

import org.slf4j.Logger;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Stopwatch;

/**
 * Creates threads form watermark calculations
 */
public class LowWatermarkCalculationJob {
    private static final Logger LOGGER = getLogger(LowWatermarkCalculationJob.class);
    private volatile Thread calculationThread;

    private final Duration pollInterval = Duration.ofMillis(300000);

    private final Consumer<Throwable> errorHandler;
    private final boolean enabled;
    private final long period;

    private final LowWatermarkCalculator lowWatermarkCalculator;

    private final LowWatermarkHolder lowWatermarkHolder;

    private final String taskUid;
    private final Clock clock;

    public LowWatermarkCalculationJob(SpannerConnectorConfig connectorConfig,
                                      Consumer<Throwable> errorHandler,
                                      LowWatermarkCalculator lowWatermarkCalculator,
                                      LowWatermarkHolder lowWatermarkHolder,
                                      String taskUid) {
        this.errorHandler = errorHandler;

        this.lowWatermarkCalculator = lowWatermarkCalculator;

        this.lowWatermarkHolder = lowWatermarkHolder;

        this.enabled = connectorConfig.isLowWatermarkEnabled();
        this.period = connectorConfig.getLowWatermarkUpdatePeriodMs();
        this.taskUid = taskUid;
        this.clock = Clock.system();
    }

    private Thread createCalculationThread() {
        Thread thread = new Thread(() -> {
            try {
                Stopwatch sw = Stopwatch.accumulating().start();
                final Metronome metronome = Metronome.sleeper(Duration.ofMillis(period), clock);
                LOGGER.info("Task {}, beginning calculation of low watermark", taskUid);
                while (true) {
                    try {
                        final Duration totalDuration = sw.stop().durations().statistics().getTotal();
                        boolean printOffsets = false;
                        if (totalDuration.toMillis() >= pollInterval.toMillis()) {
                            // Restart the stopwatch.
                            printOffsets = true;
                            LOGGER.info("Task {}, still calculating low watermark", taskUid);
                            sw = Stopwatch.accumulating().start();
                        }
                        else {
                            // Resume the existing stop watch, we haven't met the criteria yet.
                            sw.start();
                        }
                        getLowWatermark(printOffsets);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOGGER.info("Task {}, interrupted low watermark calculation", taskUid);
                        return;
                    }

                    // Now try sleeping for a predefined interval.
                    try {
                        // Sleep for pollInterval.
                        metronome.pause();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            finally {
                LOGGER.info("Task {}, ended calculation of low watermark", taskUid);
            }
        }, "SpannerConnector-WatermarkCalculationJob-Calculation");

        thread.setUncaughtExceptionHandler((t, e) -> {
            LOGGER.error("Task {}, caught exception during low watermark calculation {}", taskUid, e);
            errorHandler.accept(e);
        });

        return thread;
    }

    private void getLowWatermark(boolean printOffsets) throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            return;
        }

        Timestamp timestamp;
        do {
            timestamp = lowWatermarkCalculator.calculateLowWatermark(printOffsets);
            if (timestamp == null) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
                Thread.sleep(1);
            }
            else {
                break;
            }
        } while (true);
        lowWatermarkHolder.setLowWatermark(timestamp);
    }

    public void start() {
        if (!enabled) {
            return;
        }
        LOGGER.info("Task {}, Started low watermark calculation", taskUid);

        calculationThread = createCalculationThread();
        calculationThread.start();
    }

    public void stop() {
        if (calculationThread != null) {
            calculationThread.interrupt();
            calculationThread = null;
        }
    }

}
