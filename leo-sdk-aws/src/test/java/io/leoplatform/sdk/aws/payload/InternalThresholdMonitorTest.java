package io.leoplatform.sdk.aws.payload;

import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.config.ConnectorConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public final class InternalThresholdMonitorTest {

    private InternalThresholdMonitor monitor;

    @BeforeMethod
    private void init() {
        monitor = new InternalThresholdMonitor(testConfig(12345L), synchronousExecutor());
    }

    @AfterMethod
    void destroy() {
        monitor.end();
    }

    @Test
    public void testAddBytesLow() {
        monitor.addBytes(12344L);
        assertFalse(monitor.isFailover(), 12344L + " bytes inadvertently triggered failover");
    }

    @Test
    public void testAddBytesHigh() {
        monitor.addBytes(12345L);
        Instant start = Instant.now();
        while (!monitor.isFailover()) {
            monitor.addBytes(1L);
            if (start.plus(1, SECONDS).isBefore(Instant.now())) {
                break;
            }
        }
        assertTrue(monitor.isFailover(), "Threshold should have triggered within one second");
    }

    @Test
    public void testDisabled() {
        InternalThresholdMonitor m = new InternalThresholdMonitor(testConfig(0L), synchronousExecutor());
        m.addBytes(Long.MAX_VALUE);
        assertFalse(monitor.isFailover(), "Monitor disabled but in failover");
    }

    private ExecutorManager synchronousExecutor() {
        return new ExecutorManager() {
            @Override
            public Executor get() {
                return Executors.newSingleThreadExecutor();
            }

            @Override
            public void add(Runnable r) {
            }

            @Override
            public void end() {
            }
        };
    }

    private ConnectorConfig testConfig(Long longVal) {
        return new ConnectorConfig() {
            @Override
            public String value(String key) {
                return null;
            }

            @Override
            public Long longValue(String key) {
                return null;
            }

            @Override
            public Integer intValue(String key) {
                return null;
            }

            @Override
            public String valueOrElse(String key, String orElse) {
                return null;
            }

            @Override
            public Long longValueOrElse(String key, Long orElse) {
                return longVal;
            }

            @Override
            public Integer intValueOrElse(String key, Integer orElse) {
                return null;
            }
        };
    }
}