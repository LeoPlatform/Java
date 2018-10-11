package io.leoplatform.sdk.aws;

import io.leoplatform.sdk.AsyncWorkQueue;
import io.leoplatform.sdk.StreamStats;
import io.leoplatform.sdk.TransferStyle;
import io.leoplatform.sdk.config.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

@Singleton
public final class WorkQueues {
    private static final Logger log = LoggerFactory.getLogger(WorkQueues.class);

    private static final TransferStyle failoverStyle = TransferStyle.STORAGE;
    private final TransferStyle configuredStyle;
    private final Map<TransferStyle, AsyncWorkQueue> transferQueue;

    @Inject
    public WorkQueues(ConnectorConfig config,
                      @Named("Stream") AsyncWorkQueue kinesisQueue, @Named("Storage") AsyncWorkQueue s3Queue) {
        configuredStyle = TransferStyle.fromType(config.value("Writer"));
        transferQueue = Stream.of(kinesisQueue, s3Queue)
                .collect(collectingAndThen(
                        toMap(AsyncWorkQueue::style, identity()),
                        Collections::unmodifiableMap));
        log.info("AWS {} {} write configured", awsTypeLabel(configuredStyle), configuredStyle.style());
        log.info("AWS {} {} write configured for threshold failover", awsTypeLabel(failoverStyle), failoverStyle.style());
    }

    private String awsTypeLabel(TransferStyle style) {
        switch (style) {
            case STREAM:
                return "Kinesis";
            case STORAGE:
                return "S3";
            case BATCH:
                return "Firehose";
        }
        throw new IllegalArgumentException("Unable to recognize this transfer style: " + configuredStyle);
    }

    AsyncWorkQueue workQueue() {
        return transferQueue.get(configuredStyle);
    }

    AsyncWorkQueue failoverQueue() {
        return transferQueue.get(failoverStyle);
    }

    StreamStats endAll() {
        StreamStats storageStats = transferQueue.values().stream()
                .filter(q -> q.style() == TransferStyle.STORAGE)
                .map(AsyncWorkQueue::end)
                .findFirst()
                .orElse(emptyStats());

        StreamStats streamStats = transferQueue.values().stream()
                .filter(q -> q.style() == TransferStyle.STREAM)
                .map(AsyncWorkQueue::end)
                .findFirst()
                .orElse(emptyStats());
        return Stream.of(storageStats, streamStats)
                .reduce(this::combineStats)
                .orElse(emptyStats());
    }

    private StreamStats combineStats(StreamStats ss1, StreamStats ss2) {
        Long succ = ss1.successes() + ss2.successes();
        Long fails = ss1.failures() + ss2.failures();
        Duration dur = ss1.totalTime().plusMillis(ss2.totalTime().toMillis());
        return stats(succ, fails, dur);
    }

    private StreamStats emptyStats() {
        return stats(0L, 0L, Duration.ofMillis(0));
    }

    private StreamStats stats(Long succ, Long fails, Duration dur) {
        return new StreamStats() {
            @Override
            public Long successes() {
                return succ;
            }

            @Override
            public Long failures() {
                return fails;
            }

            @Override
            public Duration totalTime() {
                return dur;
            }
        };
    }

}
