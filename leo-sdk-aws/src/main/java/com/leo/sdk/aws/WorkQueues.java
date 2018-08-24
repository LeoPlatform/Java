package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.TransferStyle;
import com.leo.sdk.config.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.leo.sdk.TransferStyle.STORAGE;
import static com.leo.sdk.TransferStyle.fromType;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

@Singleton
public final class WorkQueues {
    private static final Logger log = LoggerFactory.getLogger(WorkQueues.class);

    private static final TransferStyle failoverStyle = STORAGE;
    private final TransferStyle configuredStyle;
    private final Map<TransferStyle, AsyncWorkQueue> transferQueue;

    @Inject
    public WorkQueues(ConnectorConfig config, List<AsyncWorkQueue> asyncQueues) {
        configuredStyle = fromType(config.value("Writer"));
        transferQueue = asyncQueues.stream()
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

    public AsyncWorkQueue workQueue() {
        return transferQueue.get(configuredStyle);
    }

    public AsyncWorkQueue failoverQueue() {
        return transferQueue.get(failoverStyle);
    }
}
