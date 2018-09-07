package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.aws.kinesis.KinesisProducerWriter;
import com.leo.sdk.aws.kinesis.KinesisQueue;
import com.leo.sdk.aws.kinesis.KinesisResults;
import com.leo.sdk.aws.payload.*;
import com.leo.sdk.aws.s3.S3Queue;
import com.leo.sdk.aws.s3.S3Results;
import com.leo.sdk.aws.s3.S3TransferManager;
import com.leo.sdk.aws.s3.S3Writer;
import com.leo.sdk.bus.LoadingBot;
import com.leo.sdk.config.ConnectorConfig;
import com.leo.sdk.payload.StreamJsonPayload;
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@Module
final class AWSModule {
    @Provides
    static PlatformStream providePlatformStream(@Named("Proxy") AsyncWorkQueue transferProxy) {
        return new AWSStream(transferProxy);
    }

    @Provides
    static WorkQueues provideWorkQueues(ConnectorConfig config, List<AsyncWorkQueue> asyncQueues) {
        return new WorkQueues(config, asyncQueues);
    }

    @Provides
    @Named("Proxy")
    static AsyncWorkQueue provideTransferProxy(WorkQueues workQueues, ThresholdMonitor thresholdMonitor) {
        return new TransferProxy(workQueues, thresholdMonitor);
    }

    @Provides
    static ThresholdMonitor provideThresholdMonitor(ConnectorConfig config) {
        return new InternalThresholdMonitor(config);
    }

    @Provides
    @Named("Stream")
    static AsyncWorkQueue provideKinesisQueue(ConnectorConfig config, CompressionWriter compression, LoadingBot bot) {
        return new KinesisQueue(config, compression, bot);
    }

    @Provides
    @Named("Storage")
    static AsyncWorkQueue provideS3Queue(CompressionWriter compression, S3Writer s3Writer) {
        return new S3Queue(compression, s3Writer);
    }

    @Named("Storage")
    static S3TransferManager provideS3TransferManager(ConnectorConfig config) {
        return new S3TransferManager(config);
    }

    @Provides
    static List<AsyncWorkQueue> provideAsyncWorkQueues(@Named("Stream") AsyncWorkQueue kinesisQueue, @Named("Storage") AsyncWorkQueue s3Queue) {
        return Stream.of(kinesisQueue, s3Queue).collect(toList());
    }

    @Provides
    static CompressionWriter provideKinesisCompression(StreamJsonPayload streamJson, ThresholdMonitor thresholdMonitor) {
        return new JSDKGzipWriter(streamJson, thresholdMonitor);
    }

    @Provides
    static StreamJsonPayload provideStreamJsonPayload(LoadingBot bot) {
        return new JacksonNewlinePayload(bot);
    }

    @Provides
    static KinesisProducerWriter provideKinesisWrite(ConnectorConfig config, KinesisResults resultsProcessor) {
        return new KinesisProducerWriter(config, resultsProcessor);
    }

    @Provides
    static KinesisResults provideKinesisResults() {
        return new KinesisResults();
    }

    @Provides
    static S3Results provideS3Results() {
        return new S3Results();
    }
}
