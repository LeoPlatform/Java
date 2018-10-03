package com.leo.sdk.oracle;

import com.leo.sdk.ExecutorManager;
import com.leo.sdk.LoadingStream;
import com.leo.sdk.StreamStats;
import com.leo.sdk.payload.EventPayload;
import oracle.jdbc.dcn.DatabaseChangeEvent;
import oracle.jdbc.dcn.DatabaseChangeListener;
import oracle.jdbc.dcn.RowChangeDescription;
import oracle.jdbc.dcn.TableChangeDescription;
import oracle.sql.ROWID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

@Singleton
public final class OracleChangeWriter implements DatabaseChangeListener {
    private static final Logger log = LoggerFactory.getLogger(OracleChangeWriter.class);

    private final LoadingStream stream;
    private final ExecutorManager executorManager;
    private final BlockingQueue<DatabaseChangeEvent> payloads = new LinkedBlockingQueue<>();
    private final Queue<CompletableFuture<Void>> pendingWrites = new LinkedList<>();
    private final AtomicBoolean running;
    private final Lock lock = new ReentrantLock();
    private final Condition changedRows = lock.newCondition();

    @Inject
    public OracleChangeWriter(LoadingStream stream, ExecutorManager executorManager) {
        this.stream = stream;
        this.executorManager = executorManager;
        this.running = new AtomicBoolean(true);
        CompletableFuture.runAsync(this::asyncWriter, executorManager.get());
    }

    @Override
    public void onDatabaseChangeNotification(DatabaseChangeEvent changeEvent) {
        log.info("Received database notification {}", changeEvent);
        if (running.get()) {
            lock.lock();
            try {
                payloads.put(changeEvent);
                changedRows.signalAll();
            } catch (InterruptedException i) {
                log.warn("Batch writer stopped unexpectedly");
                running.set(false);
            } finally {
                lock.unlock();
            }
        }
    }

    private void asyncWriter() {
        while (running.get()) {
            lock.lock();
            try {
                changedRows.await(200, MILLISECONDS);
                Queue<DatabaseChangeEvent> toWrite = new LinkedList<>();
                payloads.drainTo(toWrite);
                if (!toWrite.isEmpty()) {
                    Executor e = executorManager.get();
                    CompletableFuture<Void> cf = CompletableFuture
                            .runAsync(() -> sendToBus(toWrite), e)
                            .thenRunAsync(this::removeCompleted, e);
                    pendingWrites.add(cf);
                }

            } catch (InterruptedException e) {
                log.warn("Oracle batch change writer stopped unexpectedly");
                running.set(false);
            } finally {
                lock.unlock();
            }
        }
    }

    private void sendToBus(Queue<DatabaseChangeEvent> toWrite) {
        Map<String, Set<String>> tableChanges = toTableChanges(toWrite);
        tableChanges.forEach((t, r) -> {
            log.info("Sending rows for {}", t);
            log.info(String.join(",", r));
        });
        Optional.of(tableChanges)
                .filter(c -> !c.isEmpty())
                .map(Map::entrySet)
                .map(Collection::stream)
                .map(this::reduceEntries)
                .map(JsonObjectBuilder::build)
                .map(this::toPayload)
                .ifPresent(stream::load);
    }

    CompletableFuture<StreamStats> end() {
        running.set(false);
        lock.lock();
        try {
            changedRows.signalAll();
        } finally {
            lock.unlock();
        }

        completePendingTasks();
        return stream.end();
    }

    private Map<String, Set<String>> toTableChanges(Queue<DatabaseChangeEvent> toWrite) {
        Map<String, Set<String>> changes = new LinkedHashMap<>();
        toWrite.forEach(e -> rowsChanged(e).forEach((key, value) -> changes
                .merge(key, value, (tbl, rows) -> Stream.of(rows, value)
                        .flatMap(Set::stream)
                        .collect(toSet()))));
        return changes;
    }

    private JsonObjectBuilder reduceEntries(Stream<Entry<String, Set<String>>> s) {
        return s.reduce(Json.createObjectBuilder(),
                (b, c) -> b.add(c.getKey(), Json.createArrayBuilder(c.getValue())),
                JsonObjectBuilder::addAll);
    }

    private Map<String, Set<String>> rowsChanged(DatabaseChangeEvent changeEvent) {
        return tableChanges(changeEvent)
                .parallelStream()
                .map(this::rowChanges)
                .collect(toMap(
                        Entry::getKey,
                        Entry::getValue,
                        (r1, r2) -> Stream.of(r1, r2).flatMap(Set::stream).collect(toSet())));
    }

    private List<TableChangeDescription> tableChanges(DatabaseChangeEvent changeEvent) {
        return Optional.ofNullable(changeEvent)
                .map(DatabaseChangeEvent::getTableChangeDescription)
                .map(Arrays::asList)
                .orElse(Collections.emptyList());
    }

    private Entry<String, Set<String>> rowChanges(TableChangeDescription desc) {
        String table = tableName(desc);

        Set<String> rowIds = rowChangeDescription(desc).stream()
                .map(RowChangeDescription::getRowid)
                .map(ROWID::stringValue)
                .collect(toSet());

        return new SimpleImmutableEntry<>(table, rowIds);
    }

    private EventPayload toPayload(JsonObject jsonObject) {
        return () -> jsonObject;
    }

    private String tableName(TableChangeDescription desc) {
        return Optional.ofNullable(desc)
                .map(TableChangeDescription::getTableName)
                .orElseThrow(() -> new IllegalArgumentException("Missing table name in description " + desc));
    }

    private void removeCompleted() {
        lock.lock();
        try {
            pendingWrites.removeIf(CompletableFuture::isDone);
        } finally {
            lock.unlock();
        }
    }

    private void completePendingTasks() {
        removeCompleted();
        lock.lock();
        try {
            long inFlight = pendingWrites.parallelStream()
                    .filter(w -> !w.isDone())
                    .count();
            log.info("Waiting for {} Oracle writer task{} to complete", inFlight, inFlight == 1 ? "" : "s");
        } finally {
            lock.unlock();
        }
        while (!pendingWrites.isEmpty()) {
            lock.lock();
            try {
                changedRows.await(100, MILLISECONDS);
            } catch (InterruptedException i) {
                log.warn("Stopped with incomplete pending Oracle writer tasks");
                pendingWrites.clear();
            } finally {
                lock.unlock();
            }
            removeCompleted();
        }
    }

    private List<RowChangeDescription> rowChangeDescription(TableChangeDescription desc) {
        return Optional.ofNullable(desc)
                .map(TableChangeDescription::getRowChangeDescription)
                .map(Arrays::asList)
                .orElse(Collections.emptyList());
    }
}
