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
import java.util.stream.Stream;

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
        StreamStats ss = workQueues.workQueue().end();
        StreamStats ssFailover = workQueues.failoverQueue().end();
        return new StreamStats() {
            @Override
            public Stream<String> successIds() {
                return Stream.concat(ss.successIds(), ssFailover.successIds());
            }

            @Override
            public Stream<String> failedIds() {
                return Stream.concat(ss.failedIds(), ssFailover.failedIds());
            }

            @Override
            public Duration totalTime() {
                return ss.totalTime().plusMillis(ssFailover.totalTime().toMillis());
            }
        };
    }

    @Override
    public TransferStyle style() {
        return PROXY;
    }
}
