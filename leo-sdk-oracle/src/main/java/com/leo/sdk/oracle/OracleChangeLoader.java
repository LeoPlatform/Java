package com.leo.sdk.oracle;

import oracle.jdbc.dcn.DatabaseChangeRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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

    public CompletableFuture<WatchResults> end() {
        return end(Runnable::run);
    }

    public CompletableFuture<WatchResults> end(Executor executor) {
        if (loading.getAndSet(false)) {
            return CompletableFuture
                    .supplyAsync(this::deregister, validate(executor));
        } else {
            return noResults();
        }
    }

    private Executor validate(Executor executor) {
        return Optional.ofNullable(executor).orElse(Runnable::run);
    }

    private WatchResults deregister() {
        log.info("Stopping Oracle change loader");
        return Optional.ofNullable(dcr)
                .map(registrar::remove)
                .map(t -> new WatchResults(start, Instant.now(), t))
                .orElse(emptyResults());
    }

    private CompletableFuture<WatchResults> noResults() {
        WatchResults empty = emptyResults();
        return CompletableFuture.completedFuture(empty);
    }

    private WatchResults emptyResults() {
        Instant now = Instant.now();
        return new WatchResults(now, now, Collections.emptyList());
    }
}
