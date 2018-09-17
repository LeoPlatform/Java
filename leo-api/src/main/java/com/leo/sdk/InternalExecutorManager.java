package com.leo.sdk;

import com.leo.sdk.config.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MINUTES;

@Singleton
public class InternalExecutorManager implements ExecutorManager {
    private static final Logger log = LoggerFactory.getLogger(InternalExecutorManager.class);

    private static final AtomicInteger threadNum = new AtomicInteger();
    private final ExecutorService internalService;

    @Inject
    public InternalExecutorManager(ConnectorConfig config) {
        Integer concurrencyValue = config.intValueOrElse("ThreadPoolSize", 16);
        this.internalService = new ThreadPoolExecutor(concurrencyValue, concurrencyValue,
                5L, MINUTES, new LinkedBlockingQueue<>(), r -> {
            Thread t = new Thread(r);
            t.setName(String.format("internal-executor-manager-thread-%d", threadNum.incrementAndGet()));
            return t;
        });
        log.info("Created internally managed executor with {} maximum threads", concurrencyValue);
    }

    @Override
    public Executor get() {
        return internalService;
    }

    @Override
    public void add(Runnable r) {
        internalService.execute(r);
    }

    @Override
    public void end() {
        log.info("Stopping internally managed executor");
        internalService.shutdown();
        try {
            if (!internalService.awaitTermination(4L, MINUTES)) {
                internalService.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Could not shutdown internally managed ");
            throw new IllegalStateException("Unable to shutdown Kinesis writers", e);
        }
    }
}
