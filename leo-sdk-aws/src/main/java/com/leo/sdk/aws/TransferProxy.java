package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.StreamStats;
import com.leo.sdk.TransferStyle;
import com.leo.sdk.payload.EventPayload;
import com.leo.sdk.payload.ThresholdMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.leo.sdk.TransferStyle.PROXY;

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
        if (thresholdMonitor.isFailover()) {
            if (!failover.getAndSet(true)) {
                flushWorkQueue();
            }

            workQueues.failoverQueue().addEntity(entity);
        } else {
            if (failover.getAndSet(false)) {
                flushFailoverQueue();
            }

            workQueues.workQueue().addEntity(entity);
        }
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
        return PROXY;
    }
}
