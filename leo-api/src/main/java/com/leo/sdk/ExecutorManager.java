package com.leo.sdk;

import com.leo.sdk.config.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ExecutorManager {
    private static final Logger log = LoggerFactory.getLogger(ExecutorManager.class);

    private enum SuppliedType {INTERNAL, USER_SUPPLIED}

    private final SuppliedType type;
    private final Executor externalService;
    private final ExecutorService internalService;

    @Inject
    public ExecutorManager(ConnectorConfig config) {
        this.type = SuppliedType.INTERNAL;
        Long concurrencyValue = config.longValueOrElse("ThreadPoolSize", 16L);
        this.externalService = null;
        this.internalService = Executors.newFixedThreadPool(concurrencyValue.intValue());
        log.info("Created internally managed executor with {} maximum threads", concurrencyValue);
    }

    public ExecutorManager(Executor executor) {
        this.type = SuppliedType.USER_SUPPLIED;
        this.externalService = validate(executor);
        this.internalService = null;
        log.info("Registered user executor");
    }

    public Executor get() {
        return type == SuppliedType.USER_SUPPLIED ? externalService : internalService;
    }

    public void add(Runnable r) {
        if (type == SuppliedType.USER_SUPPLIED) {
            externalService.execute(r);
        } else {
            internalService.execute(r);
        }
    }

    public void end() {
        if (type == SuppliedType.INTERNAL) {
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
        } else {
            log.info("Not stopping registered executor");
        }
    }

    private Executor validate(Executor executor) {
        return Optional.ofNullable(executor)
                .orElseThrow(() -> new IllegalArgumentException("Invalid or missing Executor"));
    }
}
