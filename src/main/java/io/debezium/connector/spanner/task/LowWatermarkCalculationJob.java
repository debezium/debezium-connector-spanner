/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.slf4j.Logger;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.util.Stopwatch;

/**
 * Creates threads form watermark calculations
 */
public class LowWatermarkCalculationJob {
    private static final Logger LOGGER = getLogger(LowWatermarkCalculationJob.class);
    private volatile Thread mainThread;

    private volatile Thread calculationThread;

    private final Duration pollInterval = Duration.ofMillis(60000);

    private final Consumer<Throwable> errorHandler;
    private final boolean enabled;
    private final long period;

    private final LowWatermarkCalculator lowWatermarkCalculator;

    private final LowWatermarkHolder lowWatermarkHolder;

    private final Lock lock = new ReentrantLock();

    private final Condition signal = lock.newCondition();

    public LowWatermarkCalculationJob(SpannerConnectorConfig connectorConfig,
                                      Consumer<Throwable> errorHandler,
                                      LowWatermarkCalculator lowWatermarkCalculator,
                                      LowWatermarkHolder lowWatermarkHolder) {
        this.errorHandler = errorHandler;

        this.lowWatermarkCalculator = lowWatermarkCalculator;

        this.lowWatermarkHolder = lowWatermarkHolder;

        this.enabled = connectorConfig.isLowWatermarkEnabled();
        this.period = connectorConfig.getLowWatermarkUpdatePeriodMs();
    }

    private Thread createMainThread() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {

                    Thread.sleep(period);

                    if (lock.tryLock()) {
                        try {
                            signal.signal();
                        }
                        finally {
                            lock.unlock();
                        }
                    }

                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "SpannerConnector-WatermarkCalculationJob");

        thread.setUncaughtExceptionHandler((t, e) -> errorHandler.accept(e));

        return thread;
    }

    private Thread createCalculationThread() {
        Thread thread = new Thread(() -> {
            while (true) {
                Stopwatch sw = Stopwatch.accumulating().start();
                try {
                    lock.lock();
                    try {
                        signal.await();

                        final Duration totalDuration = sw.stop().durations().statistics().getTotal();
                        boolean printOffsets = false;
                        if (totalDuration.toMillis() >= pollInterval.toMillis()) {
                            // Restart the stopwatch.
                            printOffsets = true;
                            sw = Stopwatch.accumulating().start();
                        }
                        else {
                            // Resume the existing stop watch, we haven't met the criteria yet.
                            sw.start();
                        }
                        getLowWatermark(printOffsets);
                    }
                    finally {
                        lock.unlock();
                    }

                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "SpannerConnector-WatermarkCalculationJob-Calculation");

        thread.setUncaughtExceptionHandler((t, e) -> errorHandler.accept(e));

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

        calculationThread = createCalculationThread();
        calculationThread.start();

        mainThread = createMainThread();
        mainThread.start();
    }

    public void stop() {
        if (mainThread != null) {
            mainThread.interrupt();
            mainThread = null;
        }

        if (calculationThread != null) {
            calculationThread.interrupt();
            calculationThread = null;
        }
    }

}
