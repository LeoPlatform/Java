package io.leoplatform.sdk.changes;

import io.leoplatform.schema.ChangeEvent;
import io.leoplatform.sdk.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Singleton
public class AsyncChangeQueue implements SchemaChangeQueue {
    private static final Logger log = LoggerFactory.getLogger(AsyncChangeQueue.class);

    private static final Duration maxQueueTime = Duration.ofMillis(200);
    private static final int maxQueueSize = 1_000;

    private final ChangeReactor domainLoader;

    private final BlockingQueue<ChangeEvent> pendingChanges = new LinkedBlockingQueue<>();
    private final AtomicBoolean running;
    private final Lock lock = new ReentrantLock();
    private final Condition changedRows = lock.newCondition();

    @Inject
    public AsyncChangeQueue(ChangeReactor domainLoader, ExecutorManager executorManager) {
        this.domainLoader = domainLoader;
        this.running = new AtomicBoolean(true);
        CompletableFuture.runAsync(this::asyncLoader, executorManager.get());
    }

    @Override
    public void add(ChangeEvent changeEvent) {
        if (running.get()) {
            lock.lock();
            try {
                pendingChanges.add(changeEvent);
            } finally {
                lock.unlock();
            }
            signalQueue();
        }
    }

    @Override
    public void end() {
        running.set(false);
        signalQueue();
        drainQueue();
    }

    private void asyncLoader() {
        Instant lastLoad = Instant.now();
        while (running.get()) {
            lock.lock();
            try {
                changedRows.await(500, MILLISECONDS);
                while (canLoad(lastLoad)) {
                    BlockingQueue<ChangeEvent> toLoad = new LinkedBlockingQueue<>();
                    pendingChanges.drainTo(toLoad, maxQueueSize);
                    domainLoader.loadChanges(toLoad);
                }
                lastLoad = Instant.now();
            } catch (InterruptedException e) {
                log.warn("Oracle change queue stopped unexpectedly");
                running.set(false);
            } finally {
                lock.unlock();
            }
        }
    }

    private void drainQueue() {
        lock.lock();
        try {
            while (!pendingChanges.isEmpty()) {
                BlockingQueue<ChangeEvent> toLoad = new LinkedBlockingQueue<>();
                pendingChanges.drainTo(toLoad, maxQueueSize);
                domainLoader.loadChanges(toLoad);
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean canLoad(Instant lastLoad) {
        return !pendingChanges.isEmpty() && (maxElementsReached() || queueTimeExpired(lastLoad));
    }

    private boolean queueTimeExpired(Instant lastLoad) {
        return Duration.between(lastLoad, Instant.now())
            .compareTo(maxQueueTime) > 0;
    }

    private boolean maxElementsReached() {
        return pendingChanges.size() >= maxQueueSize;
    }

    private void signalQueue() {
        lock.lock();
        try {
            changedRows.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
