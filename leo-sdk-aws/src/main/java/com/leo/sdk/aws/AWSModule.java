package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.ExecutorManager;
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
import dagger.Module;
import dagger.Provides;

import javax.inject.Named;
import javax.inject.Singleton;

@Module
final class AWSModule {
    @Singleton
    @Provides
    static PlatformStream providePlatformStream(@Named("Proxy") AsyncWorkQueue transferProxy, ExecutorManager executorManager) {
        return new AWSStream(transferProxy, executorManager);
    }

    @Singleton
    @Provides
    static WorkQueues provideWorkQueues(ConnectorConfig config, @Named("Stream") AsyncWorkQueue kinesisQueue, @Named("Storage") AsyncWorkQueue s3Queue) {
        return new WorkQueues(config, kinesisQueue, s3Queue);
    }

    @Singleton
    @Provides
    @Named("Proxy")
    static AsyncWorkQueue provideTransferProxy(WorkQueues workQueues, ThresholdMonitor thresholdMonitor) {
        return new TransferProxy(workQueues, thresholdMonitor);
    }

    @Singleton
    @Provides
    @Named("Stream")
    static AsyncWorkQueue provideKinesisQueue(ConnectorConfig config, ExecutorManager executorManager,
                                              CompressionWriter compression, KinesisProducerWriter writer) {
        return new KinesisQueue(config, executorManager, compression, writer);
    }

    @Singleton
    @Provides
    @Named("Storage")
    static AsyncWorkQueue provideS3Queue(ExecutorManager executorManager,
                                         CompressionWriter compression, S3Writer s3Writer) {
        return new S3Queue(executorManager, compression, s3Writer);
    }

    @Singleton
    @Provides
    static ThresholdMonitor provideThresholdMonitor(ConnectorConfig config, ExecutorManager executorManager) {
        return new InternalThresholdMonitor(config, executorManager);
    }

    @Singleton
    @Provides
    static S3TransferManager provideS3TransferManager(ConnectorConfig config) {
        return new S3TransferManager(config);
    }

    @Singleton
    @Provides
    static CompressionWriter provideKinesisCompression(StreamJsonPayload streamJson, ThresholdMonitor thresholdMonitor) {
        return new JCraftGzipWriter(streamJson, thresholdMonitor);
    }

    @Singleton
    @Provides
    static StreamJsonPayload provideStreamJsonPayload(LoadingBot bot) {
        return new JacksonPayload(bot);
    }

    @Singleton
    @Provides
    static KinesisProducerWriter provideKinesisWrite(ConnectorConfig config, ExecutorManager executorManager, KinesisResults resultsProcessor) {
        return new KinesisProducerWriter(config, executorManager, resultsProcessor);
    }

    @Singleton
    @Provides
    static KinesisResults provideKinesisResults() {
        return new KinesisResults();
    }

    @Singleton
    @Provides
    static S3Results provideS3Results(CompressionWriter compression, KinesisProducerWriter kinesis) {
        return new S3Results(compression, kinesis);
    }
}
