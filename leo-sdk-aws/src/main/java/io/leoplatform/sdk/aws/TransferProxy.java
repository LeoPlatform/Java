package io.leoplatform.sdk.aws;

import io.leoplatform.sdk.AsyncWorkQueue;
import io.leoplatform.sdk.StreamStats;
import io.leoplatform.sdk.TransferStyle;
import io.leoplatform.sdk.payload.EventPayload;
import io.leoplatform.sdk.payload.ThresholdMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public final class TransferProxy implements AsyncWorkQueue {
    private static final Logger log = LoggerFactory.getLogger(TransferProxy.class);

    private final WorkQueues workQueues;
    private final ThresholdMonitor thresholdMonitor;
    private final AtomicBoolean failover = new AtomicBoolean(false);

    @Inject
    public TransferProxy(WorkQueues workQueues, ThresholdMonitor thresholdMonitor) {
        this.workQueues = workQueues;
        this.thresholdMonitor = thresholdMonitor;
        if (workQueues.workQueue().style() == workQueues.failoverQueue().style()) {
            thresholdMonitor.end();
        }
    }

    @Override
    public void addEntity(EventPayload entity) {
        AsyncWorkQueue queue;
        if (thresholdMonitor.isFailover()) {
            if (!failover.getAndSet(true)) {
                flushWorkQueue();
            }
            queue = workQueues.failoverQueue();
        } else {
            if (failover.getAndSet(false)) {
                flushFailoverQueue();
            }
            queue = workQueues.workQueue();
        }
        queue.addEntity(entity);
    }

    @Override
    public void flush() {
        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(this::flushFailoverQueue);
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(this::flushWorkQueue);
        CompletableFuture.allOf(cf1, cf2)
                .join();
        log.info("Flushed all work queues");
    }

    private void flushWorkQueue() {
        log.info("Flushing Kinesis payloads");
        workQueues.workQueue().flush();
    }

    private void flushFailoverQueue() {
        log.info("Flushing S3 uploads");
        workQueues.failoverQueue().flush();
    }

    @Override
    public StreamStats end() {
        log.info("Stopping transfer proxy");
        flush();
        thresholdMonitor.end();
        return workQueues.endAll();
    }

    @Override
    public TransferStyle style() {
        return TransferStyle.PROXY;
    }
}
