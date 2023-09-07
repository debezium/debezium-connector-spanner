/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.concurrent.locks.Condition;
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

    private final Duration pollInterval = Duration.ofMillis(300000);

    private final Consumer<Throwable> errorHandler;
    private final boolean enabled;
    private final long period;

    private final LowWatermarkCalculator lowWatermarkCalculator;

    private final LowWatermarkHolder lowWatermarkHolder;

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition signal = lock.newCondition();
    private final String taskUid;

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
    }

    private Thread createMainThread() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {

                    Thread.sleep(period);

                    if (lock.tryLock()) {
                        signal.signal();
                    }

                }
                catch (InterruptedException e) {
                    LOGGER.info("Task {}, Interrupted low watermark calculation main thread", taskUid);
                    Thread.currentThread().interrupt();
                    return;
                }
                finally {
                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                }
            }
        }, "SpannerConnector-WatermarkCalculationJob");

        thread.setUncaughtExceptionHandler((t, e) -> {
            LOGGER.error("Task {}, caught exception during low watermark calculation {}", taskUid, e);
            errorHandler.accept(e);
        });

        return thread;
    }

    private Thread createCalculationThread() {
        Thread thread = new Thread(() -> {
            Stopwatch sw = Stopwatch.accumulating().start();
            while (true) {
                try {
                    lock.lock();
                    signal.await();

                    final Duration totalDuration = sw.stop().durations().statistics().getTotal();
                    boolean printOffsets = false;
                    if (totalDuration.toMillis() >= pollInterval.toMillis()) {
                        // Restart the stopwatch.
                        printOffsets = true;
                        sw = Stopwatch.accumulating().start();
                        LOGGER.info("Task {}, still calculating low watermark", taskUid);
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
                finally {
                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                }
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
