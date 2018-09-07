package com.leo.sdk.oracle;

import com.leo.sdk.PlatformStream;
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
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class OracleChangeWriter implements DatabaseChangeListener {
    private static final Logger log = LoggerFactory.getLogger(OracleChangeWriter.class);

    private final PlatformStream stream;
    private final Map<String, Set<String>> changedRows = new HashMap<>();
    private final Lock lock = new ReentrantLock();
    private final ScheduledExecutorService writer = Executors.newSingleThreadScheduledExecutor();

    @Inject
    public OracleChangeWriter(PlatformStream stream) {
        writer.scheduleWithFixedDelay(periodicWrite(), 100L, 100L, MILLISECONDS);
        this.stream = stream;
    }

    @Override
    public void onDatabaseChangeNotification(DatabaseChangeEvent changeEvent) {
        log.info("Received database notification {}", changeEvent);
        Optional.of(rowsChanged(changeEvent))
                .filter(r -> !r.isEmpty())
                .ifPresent(changes -> {
                    lock.lock();
                    try {
                        changes.forEach((key, value) -> changedRows
                                .merge(key, value, (tbl, rows) -> Stream.of(rows, value)
                                        .flatMap(Set::stream)
                                        .collect(toSet())));
                    } finally {
                        lock.unlock();
                    }
                });
    }

    private Runnable periodicWrite() {
        return () -> {
            Map<String, Set<String>> changes;
            lock.lock();
            try {
                changes = new HashMap<>(changedRows);
                changedRows.clear();
            } finally {
                lock.unlock();
            }

            Optional.of(changes)
                    .filter(c -> !c.isEmpty())
                    .map(Map::entrySet)
                    .map(Collection::stream)
                    .map(this::reduceEntries)
                    .map(JsonObjectBuilder::build)
                    .map(this::toPayload)
                    .ifPresent(stream::load);
        };
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

    public CompletableFuture<StreamStats> end() {
        writer.shutdownNow();
        try {
            writer.awaitTermination(30L, SECONDS);
            writer.shutdownNow();
        } catch (InterruptedException e) {
            log.warn("Could not shutdown background change writer");
        }
        return stream.end();
    }

    private String tableName(TableChangeDescription desc) {
        return Optional.ofNullable(desc)
                .map(TableChangeDescription::getTableName)
                .orElseThrow(() -> new IllegalArgumentException("Missing table name in description " + desc));
    }

    private List<RowChangeDescription> rowChangeDescription(TableChangeDescription desc) {
        return Optional.ofNullable(desc)
                .map(TableChangeDescription::getRowChangeDescription)
                .map(Arrays::asList)
                .orElse(Collections.emptyList());
    }
}
