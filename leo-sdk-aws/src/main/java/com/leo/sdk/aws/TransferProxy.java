package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.StreamStats;
import com.leo.sdk.TransferStyle;
import com.leo.sdk.config.ConnectorConfig;
import com.leo.sdk.payload.EntityPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

import static com.leo.sdk.TransferStyle.PROXY;

public class TransferProxy implements AsyncWorkQueue {
    private static final Logger log = LoggerFactory.getLogger(TransferProxy.class);
    private final TransferStyle style = PROXY;
    private final AsyncWorkQueue transferQueue;

    @Inject
    public TransferProxy(ConnectorConfig config, List<AsyncWorkQueue> asyncQueues) {
        TransferStyle type = TransferStyle.fromType(config.value("Writer"));
        this.transferQueue = workQueueStyle(asyncQueues, type);

        String awsType = transferTypeLabel();
        log.info("AWS {} {} transfer configured", awsType, transferQueue.style().style());
    }

    private AsyncWorkQueue workQueueStyle(List<AsyncWorkQueue> workQueues, TransferStyle type) {
        return workQueues.stream()
                .filter(q -> q.style() == type)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown uploader " + type));
    }

    @Override
    public void addEntity(EntityPayload entity) {
        transferQueue.addEntity(entity);
    }

    @Override
    public StreamStats end() {
        return transferQueue.end();
    }

    @Override
    public TransferStyle style() {
        return style;
    }

    private String transferTypeLabel() {
        return Optional.of(transferQueue)
                .map(AsyncWorkQueue::style)
                .map(s -> {
                    switch (s) {
                        case STREAM:
                            return "Kinesis";
                        case STORAGE:
                            return "S3";
                        case BATCH:
                            return "Firehose";
                    }
                    throw new IllegalArgumentException("Cannot proxy this transfer style: " + transferQueue.style());
                })
                .orElseThrow(() -> new IllegalArgumentException("Unknown transfer style: " + transferQueue.style()));
    }
}
