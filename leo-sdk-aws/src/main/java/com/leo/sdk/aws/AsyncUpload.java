package com.leo.sdk.aws;

import com.leo.sdk.StreamStats;
import com.leo.sdk.Uploader;
import com.leo.sdk.WriteQueue;
import com.leo.sdk.config.ConnectorConfig;
import com.leo.sdk.payload.SimplePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

public class AsyncUpload {
    private static final Logger log = LoggerFactory.getLogger(AsyncUpload.class);

    private final WriteQueue writeQueue;

    @Inject
    public AsyncUpload(ConnectorConfig config, List<WriteQueue> queues) {
        Uploader type = Uploader.fromType(config.value("Writer"));
        this.writeQueue = queues.stream()
                .filter(q -> q.type() == type)
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Unknown uploader " + type));
        String awsType = Optional.of(type)
                .filter(t -> t == Uploader.STREAM)
                .map(t -> "Kinesis")
                .orElse("Unknown");
        log.info("AWS {} {} loader configured", awsType, writeQueue.type().style());
    }

    void upload(SimplePayload entity) {
        writeQueue.addEntity(entity);
    }

    StreamStats end() {
        return writeQueue.end();
    }
}
