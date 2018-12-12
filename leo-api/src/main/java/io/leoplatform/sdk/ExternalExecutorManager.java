package io.leoplatform.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Optional;
import java.util.concurrent.Executor;

@Singleton
public final class ExternalExecutorManager implements ExecutorManager {
    private static final Logger log = LoggerFactory.getLogger(ExternalExecutorManager.class);

    private final Executor externalService;

    @Inject
    public ExternalExecutorManager(Executor executor) {
        this.externalService = validate(executor);
        log.info("Registered user executor");
    }

    @Override
    public Executor get() {
        return externalService;
    }

    @Override
    public void add(Runnable r) {
        externalService.execute(r);
    }

    @Override
    public void end() {
        log.info("Not stopping registered executor");
    }

    private Executor validate(Executor executor) {
        return Optional.ofNullable(executor)
                .orElseThrow(() -> new IllegalArgumentException("Invalid or missing Executor"));
    }
}
