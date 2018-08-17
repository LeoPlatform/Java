package com.leo.sdk.oracle;

import com.leo.sdk.PlatformStream;
import com.leo.sdk.StreamStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.SECONDS;

public class LeoOracleChanges {
    private static final Logger log = LoggerFactory.getLogger(LeoOracleChanges.class);

    private final OracleChangeWriter changeWriter;
    private final OracleChangesRegistrar registrar;
    private ExecutorService executorService = null;

    public LeoOracleChanges(OracleChangesSource source, PlatformStream destination) {
        this(source, destination, new SimpleOracleChangeListener());
    }

    public LeoOracleChanges(OracleChangesSource source, PlatformStream destination, OracleChangeListener listener) {
        this.changeWriter = new OracleChangeWriter(destination);
        this.registrar = new OracleChangesRegistrar(source, listener);
        this.executorService = Executors.newSingleThreadExecutor();
        start(executorService);
    }

    public LeoOracleChanges(OracleChangesSource source, PlatformStream destination, Executor executor) {
        this(source, destination, new SimpleOracleChangeListener(), executor);
    }

    public LeoOracleChanges(OracleChangesSource source, PlatformStream destination, OracleChangeListener listener, Executor executor) {
        this.changeWriter = new OracleChangeWriter(destination);
        this.registrar = new OracleChangesRegistrar(source, listener);
        start(Optional.ofNullable(executor)
                .orElseThrow(() -> new IllegalArgumentException("Missing or invalid executor")));

    }

    private void start(Executor executor) {
        executor.execute(registrar.listen(changeWriter, executor));
    }

    public CompletableFuture<WatchResults> end() {
        return end(executorService);
    }

    public CompletableFuture<WatchResults> end(Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            WatchResults results = registrar.end();
            CompletableFuture<StreamStats> futureStats = changeWriter.end()
                    .whenComplete((streamStats, throwable) -> logStats(streamStats));
            Optional.ofNullable(executorService)
                    .ifPresent(this::shutdownExecutorService);
            futureStats.join();
            return results;
        }, executor);
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
