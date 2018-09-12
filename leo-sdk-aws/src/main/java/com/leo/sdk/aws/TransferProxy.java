package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.StreamStats;
import com.leo.sdk.TransferStyle;
import com.leo.sdk.aws.payload.ThresholdMonitor;
import com.leo.sdk.payload.EventPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.leo.sdk.TransferStyle.PROXY;

@Singleton
public final class TransferProxy implements AsyncWorkQueue {
    private static final Logger log = LoggerFactory.getLogger(TransferProxy.class);

    private final WorkQueues workQueues;
    private final ThresholdMonitor thresholdMonitor;

    @Inject
    public TransferProxy(WorkQueues workQueues, ThresholdMonitor thresholdMonitor) {
        this.workQueues = workQueues;
        this.thresholdMonitor = thresholdMonitor;
    }

    @Override
    public void addEntity(EventPayload entity) {
        if (thresholdMonitor.isFailover()) {
            workQueues.failoverQueue().addEntity(entity);
        } else {
            workQueues.workQueue().addEntity(entity);
        }
    }

    @Override
    public StreamStats end() {
        thresholdMonitor.end();
        CompletableFuture<StreamStats> work = CompletableFuture.supplyAsync(() -> workQueues.workQueue().end());
        CompletableFuture<StreamStats> fail = CompletableFuture.supplyAsync(() -> workQueues.failoverQueue().end());
        CompletableFuture.allOf(work, fail)
                .join();

        try {
            StreamStats ss = work.get();
            StreamStats ssFailover = fail.get();
            Long succ = ss.successes() + ssFailover.successes();
            Long fails = ss.failures() + ssFailover.failures();
            Duration dur = ss.totalTime().plusMillis(ssFailover.totalTime().toMillis());
            return stats(succ, fails, dur);
        } catch (ExecutionException | InterruptedException e) {
            return stats(0L, 0L, Duration.ofMillis(0L));
        }
    }

    private StreamStats stats(Long successes, Long failures, Duration dur) {
        return new StreamStats() {
            @Override
            public Long successes() {
                return successes;
            }

            @Override
            public Long failures() {
                return failures;
            }

            @Override
            public Duration totalTime() {
                return dur;
            }
        };
    }

    @Override
    public TransferStyle style() {
        return PROXY;
    }
}
