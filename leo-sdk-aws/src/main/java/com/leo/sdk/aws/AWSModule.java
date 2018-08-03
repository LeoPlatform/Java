package com.leo.sdk.aws;

import com.leo.sdk.PlatformStream;
import com.leo.sdk.WriteQueue;
import com.leo.sdk.aws.kinesis.KinesisCompression;
import com.leo.sdk.aws.kinesis.KinesisQueue;
import com.leo.sdk.aws.kinesis.KinesisResults;
import com.leo.sdk.aws.kinesis.KinesisWrite;
import com.leo.sdk.aws.payload.JSDKGzipPayload;
import com.leo.sdk.aws.payload.JacksonNewlinePayload;
import com.leo.sdk.config.ConnectorConfig;
import com.leo.sdk.payload.StreamJsonPayload;
import dagger.Module;
import dagger.Provides;

import java.util.Collections;
import java.util.List;

@Module
public class AWSModule {

    @Provides
    public static KinesisResults provideKinesisResults() {
        return new KinesisResults();
    }

    @Provides
    public static KinesisWrite provideKinesisWrite(ConnectorConfig config, KinesisResults resultsProcessor) {
        return new KinesisWrite(config, resultsProcessor);
    }

    @Provides
    public static StreamJsonPayload provideStreamJsonPayload() {
        return new JacksonNewlinePayload();
    }

    @Provides
    public static KinesisCompression provideKinesisCompression(StreamJsonPayload streamJson) {
        return new JSDKGzipPayload(streamJson);
    }

    @Provides
    public static List<WriteQueue> provideWriteQueueList(KinesisCompression compression, KinesisWrite kinesisWriter) {
        return Collections.singletonList(new KinesisQueue(compression, kinesisWriter));
    }

//    @Provides
//    static ConnectorConfig provideConnectorConfig() {
//        return new FileConfig();
//    }

    @Provides
    public static AsyncUpload provideAsyncUpload(ConnectorConfig config, List<WriteQueue> queues) {
        return new AsyncUpload(config, queues);
    }

    @Provides
    public static PlatformStream provideAwsStream(AsyncUpload AsyncUpload) {
        return new AWSStream(AsyncUpload);
    }
}
