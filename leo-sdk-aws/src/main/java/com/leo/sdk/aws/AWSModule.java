package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.aws.kinesis.KinesisCompression;
import com.leo.sdk.aws.kinesis.KinesisProducerWriter;
import com.leo.sdk.aws.kinesis.KinesisQueue;
import com.leo.sdk.aws.kinesis.KinesisResults;
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
    public static PlatformStream provideAwsStream(TransferProxy transferProxy) {
        return new AWSStream(transferProxy);
    }

    public static TransferProxy provideTransferProxy(ConnectorConfig config, List<AsyncWorkQueue> asyncQueues) {
        return new TransferProxy(config, asyncQueues);
    }

    public static KinesisQueue provideKinesisQueue(KinesisCompression compression, KinesisProducerWriter kinesisWriter) {
        return new KinesisQueue(compression, kinesisWriter);
    }

    @Provides
    public static List<AsyncWorkQueue> provideWorkQueues(KinesisQueue kinesisQueue) {
        return Collections.singletonList(kinesisQueue);
    }

    @Provides
    public static KinesisCompression provideKinesisCompression(StreamJsonPayload streamJson) {
        return new JSDKGzipPayload(streamJson);
    }

    @Provides
    public static StreamJsonPayload provideStreamJsonPayload() {
        return new JacksonNewlinePayload();
    }

    @Provides
    public static KinesisProducerWriter provideKinesisWrite(ConnectorConfig config, KinesisResults resultsProcessor) {
        return new KinesisProducerWriter(config, resultsProcessor);
    }

    @Provides
    public static KinesisResults provideKinesisResults() {
        return new KinesisResults();
    }
}
