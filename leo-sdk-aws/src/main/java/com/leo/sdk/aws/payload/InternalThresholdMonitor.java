package com.leo.sdk.aws.payload;

import com.leo.sdk.config.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.math.BigDecimal;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.math.RoundingMode.HALF_EVEN;
import static java.math.RoundingMode.HALF_UP;
import static java.util.concurrent.TimeUnit.SECONDS;

public class InternalThresholdMonitor implements ThresholdMonitor {
    private static final Logger log = LoggerFactory.getLogger(InternalThresholdMonitor.class);

    private final long maxBytesPerSecond;
    private final BigDecimal warningThreshold;
    private final AtomicLong currentLevel = new AtomicLong();
    private final AtomicBoolean failover = new AtomicBoolean(false);
    private final ScheduledExecutorService monitor;

    @Inject
    public InternalThresholdMonitor(ConnectorConfig config) {
        maxBytesPerSecond = config.longValueOrElse("Stream.BytesPerSecondFailover", 500000000L);
        warningThreshold = new BigDecimal(maxBytesPerSecond)
                .multiply(new BigDecimal(".8"))
                .setScale(0, HALF_UP);
        monitor = Executors.newScheduledThreadPool(2);
        if (maxBytesPerSecond > 0) {
            monitor.scheduleWithFixedDelay(thresholdCheck(), 1, 1, SECONDS);
        }
    }

    @Override
    public void addBytes(long bytes) {
        currentLevel.addAndGet(bytes);
    }

    @Override
    public boolean isFailover() {
        return failover.get();
    }

    @Override
    public void end() {
        monitor.shutdown();
        try {
            if (!monitor.awaitTermination(1L, SECONDS)) {
                monitor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Could not shutdown threshold monitor");
        }
    }

    private boolean wasOverThreshold() {
        return currentLevel.getAndSet(0) > maxBytesPerSecond;
    }

    private boolean isOverThreshold() {
        return currentLevel.get() > maxBytesPerSecond;
    }

    private Runnable thresholdCheck() {
        return () -> {
            if (wasOverThreshold()) {
                boolean wasFailover = failover.getAndSet(true);
                if (!wasFailover) {
                    log.warn("Bytes per second exceeded {}: failover enabled", maxBytesPerSecond);
                }
            } else {
                monitor.schedule(clearThresholdCheck(), 10, SECONDS);
                BigDecimal level = new BigDecimal(currentLevel.get());
                if (level.compareTo(warningThreshold) > 0) {
                    BigDecimal percentageOfThreshold = level
                            .divide(new BigDecimal(maxBytesPerSecond), HALF_EVEN)
                            .movePointRight(2)
                            .setScale(0, HALF_EVEN);
                    log.warn("Bytes per second are currently {}% of your failover threshold", percentageOfThreshold);
                }
            }
        };
    }

    private Runnable clearThresholdCheck() {
        return () -> {
            if (failover.get() && isOverThreshold()) {
                log.warn("Failover remains in place");
            } else if (failover.get()) {
                failover.set(false);
                log.info("Cleared failover");
            }
        };
    }
}
