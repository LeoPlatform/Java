package io.leoplatform.sdk.oracle;

import io.leoplatform.schema.ChangeEvent;
import io.leoplatform.schema.Field;
import io.leoplatform.schema.Op;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.changes.SchemaChangeQueue;
import oracle.jdbc.dcn.DatabaseChangeEvent;
import oracle.jdbc.dcn.DatabaseChangeListener;
import oracle.jdbc.dcn.RowChangeDescription;
import oracle.jdbc.dcn.RowChangeDescription.RowOperation;
import oracle.jdbc.dcn.TableChangeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.leoplatform.schema.FieldType.STRING;
import static io.leoplatform.schema.Source.ORACLE;
import static java.util.stream.Collectors.*;

@Singleton
public final class OracleChangeWriter implements DatabaseChangeListener {
    private static final Logger log = LoggerFactory.getLogger(OracleChangeWriter.class);

    private final SchemaChangeQueue changeQueue;
    private final BlockingQueue<DatabaseChangeEvent> payloads = new LinkedBlockingQueue<>();
    private final AtomicBoolean running;
    private final Lock lock = new ReentrantLock();
    private final Condition changedRows = lock.newCondition();

    @Inject
    public OracleChangeWriter(SchemaChangeQueue changeQueue, ExecutorManager executorManager) {
        this.changeQueue = changeQueue;
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
            } catch (InterruptedException i) {
                log.warn("Batch writer stopped unexpectedly");
                running.set(false);
            } finally {
                lock.unlock();
            }
            signalAll();
        }
    }

    private void asyncWriter() {
        while (running.get()) {
            Queue<DatabaseChangeEvent> toWrite = new LinkedList<>();
            lock.lock();
            try {
                changedRows.await();
                payloads.drainTo(toWrite);
            } catch (InterruptedException e) {
                log.warn("Oracle batch change writer stopped unexpectedly");
                running.set(false);
            } finally {
                lock.unlock();
            }
            if (!toWrite.isEmpty()) {
                sendToChangeQueue(toWrite);
            }
        }
    }

    private void sendToChangeQueue(Queue<DatabaseChangeEvent> toWrite) {
        toChangeEvents(toWrite).forEach(change -> {
            List<String> rowIds = change.getFields().stream()
                .map(Field::getValue)
                .collect(toList());
            log.info("Sending notification for {} {} changes", rowIds.size(), change.getName());
            log.debug("ROWIDs changed: {}", String.join(",", rowIds));
            changeQueue.add(change);
        });
    }

    void end() {
        running.set(false);
        signalAll();
        drainBuffer();
        changeQueue.end();
    }

    private void signalAll() {
        lock.lock();
        try {
            changedRows.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private Collection<ChangeEvent> toChangeEvents(Queue<DatabaseChangeEvent> changeEvents) {
        return changeEvents.parallelStream()
            .flatMap(this::tableChanges)
            .map(this::rowChanges)
            .flatMap(Set::stream)
            .collect(toConcurrentMap(
                ChangeEvent::getName,
                Function.identity(),
                this::combineEvents)
            )
            .values();
    }

    private ChangeEvent combineEvents(ChangeEvent c1, ChangeEvent c2) {
        List<Field> f = Stream.of(c1.getFields(), c2.getFields())
            .flatMap(List::stream)
            .distinct()
            .collect(toList());
        return new ChangeEvent(c1.getSource(), c1.getOp(), c1.getName(), f);
    }

    private Stream<TableChangeDescription> tableChanges(DatabaseChangeEvent changeEvent) {
        return Optional.of(changeEvent)
            .map(DatabaseChangeEvent::getTableChangeDescription)
            .map(Stream::of)
            .orElse(Stream.empty());
    }

    private Set<ChangeEvent> rowChanges(TableChangeDescription desc) {
        String table = tableName(desc);

        return rowChangeDescription(desc).stream()
            .map(rowDesc -> {
                Field f = new Field("ROWID", STRING, rowDesc.getRowid().stringValue());
                return new SimpleImmutableEntry<>(getOp(rowDesc), f);
            })
            .map(e -> new ChangeEvent(ORACLE, e.getKey(), table, Collections.singletonList(e.getValue())))
            .collect(toSet());
    }

    private Op getOp(RowChangeDescription desc) {
        RowOperation oracleOp = Optional.of(desc)
            .map(RowChangeDescription::getRowOperation)
            .orElse(RowOperation.UPDATE);
        switch (oracleOp) {
            case INSERT:
                return Op.INSERT;
            case UPDATE:
                return Op.UPDATE;
            case DELETE:
                return Op.DELETE;
        }
        return Op.UPDATE;
    }

    private String tableName(TableChangeDescription desc) {
        return Optional.ofNullable(desc)
            .map(TableChangeDescription::getTableName)
            .orElseThrow(() -> new IllegalArgumentException("Missing table name in description " + desc));
    }

    private void drainBuffer() {
        Queue<DatabaseChangeEvent> toWrite = new LinkedList<>();
        lock.lock();
        try {
            payloads.drainTo(toWrite);
        } finally {
            lock.unlock();
        }
        if (!toWrite.isEmpty()) {
            sendToChangeQueue(toWrite);
        }
    }

    private List<RowChangeDescription> rowChangeDescription(TableChangeDescription desc) {
        return Optional.ofNullable(desc)
            .map(TableChangeDescription::getRowChangeDescription)
            .map(Arrays::asList)
            .orElse(Collections.emptyList());
    }
}
