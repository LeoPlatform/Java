package com.leo.sdk.oracle;

import oracle.jdbc.dcn.DatabaseChangeRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public final class OracleChangeLoader {
    private static final Logger log = LoggerFactory.getLogger(OracleChangeLoader.class);

    private final OracleChangeRegistrar registrar;
    private DatabaseChangeRegistration dcr = null;
    private final AtomicBoolean loading = new AtomicBoolean(false);
    private Instant start = null;

    @Inject
    public OracleChangeLoader(OracleChangeRegistrar registrar) {
        this.registrar = registrar;
    }

    public void register(OracleChangeDestination destination) {
        register(destination, Runnable::run);
    }

    public void register(OracleChangeDestination destination, Executor executor) {
        if (!loading.getAndSet(true)) {
            start = Instant.now();
            this.dcr = registrar.create(destination, validate(executor));
            loading.set(false);
        }
    }

    public WatchResults deregister() {
        log.info("Stopping Oracle change loader");
        return Optional.ofNullable(dcr)
                .map(registrar::remove)
                .map(t -> new WatchResults(start, Instant.now(), t))
                .orElse(emptyResults());
    }

    public WatchResults end() {
        return end(Runnable::run);
    }

    public WatchResults end(Executor executor) {
        if (loading.getAndSet(false)) {
            return Optional.ofNullable(registrar)
                    .map(OracleChangeRegistrar::tables)
                    .map(t -> new WatchResults(start, Instant.now(), t))
                    .orElse(emptyResults());
        } else {
            return emptyResults();
        }
    }

    private Executor validate(Executor executor) {
        return Optional.ofNullable(executor).orElse(Runnable::run);
    }

    private WatchResults emptyResults() {
        Instant now = Instant.now();
        return new WatchResults(now, now, Collections.emptyList());
    }
}
