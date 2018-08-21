package com.leo.sdk.oracle;

import com.leo.sdk.PlatformStream;
import com.leo.sdk.StreamStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class OracleChangeLoader {
    private static final Logger log = LoggerFactory.getLogger(OracleChangeLoader.class);

    private final OracleChangeWriter changeWriter;
    private final OracleChangesRegistrar registrar;
    private final ExecutorService executorService;

    private final AtomicBoolean loading = new AtomicBoolean(false);

    public OracleChangeLoader(OracleChangeSource source, PlatformStream destination) {
        this(source, new SimpleOracleChangeListener(), destination);
    }

    public OracleChangeLoader(OracleChangeSource source, OracleChangeListener listener, PlatformStream destination) {
        this.changeWriter = new OracleChangeWriter(destination);
        this.registrar = new OracleChangesRegistrar(source, listener);
        this.executorService = Executors.newSingleThreadExecutor();
        executorService.execute(registrar.listen(changeWriter, executorService));
        loading.set(true);
    }

    public OracleChangeLoader(OracleChangeSource source, PlatformStream destination, Executor executor) {
        this(source, new SimpleOracleChangeListener(), destination, executor);
    }

    public OracleChangeLoader(OracleChangeSource source, OracleChangeListener listener, PlatformStream destination, Executor executor) {
        this.changeWriter = new OracleChangeWriter(destination);
        this.registrar = new OracleChangesRegistrar(source, listener);
        this.executorService = null;
        Optional.ofNullable(executor)
                .orElseThrow(() -> new IllegalArgumentException("Missing or invalid executor"))
                .execute(registrar.listen(changeWriter, executor));
        loading.set(true);
    }

    public CompletableFuture<WatchResults> end() {
        if (loading.getAndSet(false)) {
            return CompletableFuture.supplyAsync(this::stopRegistrar);
        } else {
            return noResults();
        }
    }

    public CompletableFuture<WatchResults> end(Executor executor) {
        if (loading.getAndSet(false)) {
            return CompletableFuture.supplyAsync(this::stopRegistrar, executor);
        } else {
            return noResults();
        }
    }

    private WatchResults stopRegistrar() {
        log.info("Stopping Oracle change loader");
        WatchResults results = registrar.end();
        Optional.ofNullable(executorService)
                .ifPresent(this::shutdownExecutorService);
        return stopChangeWriter(results);
    }

    private WatchResults stopChangeWriter(WatchResults results) {
        changeWriter.end()
                .whenComplete((streamStats, throwable) -> logStats(streamStats))
                .join();
        log.info("Oracle change loader stopped");
        return results;
    }

    private void shutdownExecutorService(ExecutorService s) {

        s.shutdown();
        try {
            if (!s.awaitTermination(30L, SECONDS)) {
                s.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Could not shutdown listener service");
        }
    }

    private CompletableFuture<WatchResults> noResults() {
        Instant now = Instant.now();
        WatchResults empty = new WatchResults(now, now, Collections.emptyList());
        return CompletableFuture.completedFuture(empty);
    }

    private void logStats(StreamStats stats) {
        long successCount = stats.successIds().count();
        if (successCount > 0) {
            log.info("{} records successfully processed", successCount);
        }
        long failCount = stats.failedIds().count();
        if (failCount > 0) {
            log.warn("{} records failed to load", successCount);
        }
    }
}
