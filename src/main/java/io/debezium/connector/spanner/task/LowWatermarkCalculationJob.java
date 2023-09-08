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
    private final Duration sleepInterval = Duration.ofMillis(100);

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
                while (!Thread.currentThread().isInterrupted()) {
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
                        LOGGER.info("Task {}, getting low watermark", taskUid);
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
        Timestamp timestamp;
        final Metronome metronome = Metronome.sleeper(Duration.ofMillis(100), clock);
        do {
            try {
                timestamp = lowWatermarkCalculator.calculateLowWatermark(printOffsets);
                if (timestamp == null) {
                    LOGGER.warn("Task {}, failed to retrieve low watermark", taskUid);
                    metronome.pause();
                }
                else {
                    break;
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        } while (!Thread.currentThread().isInterrupted());
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

            final Metronome metronome = Metronome.sleeper(sleepInterval, clock);
            LOGGER.info("Task {}, stopping low watermark calculation thread ", taskUid);
            while (!this.calculationThread.getState().equals(Thread.State.TERMINATED)) {
                try {
                    // Sleep for sleepInterval.
                    metronome.pause();

                    LOGGER.info("Task {}, interrupting low watermark calculation thread again", taskUid);
                    this.calculationThread.interrupt();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            LOGGER.info("Task {}, stopped low watermark calculation thread ", taskUid);
            calculationThread = null;
        }
    }

}
