package io.leoplatform.sdk.aws;

import dagger.Module;
import dagger.Provides;
import io.leoplatform.sdk.AsyncWorkQueue;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.LoadingStream;
import io.leoplatform.sdk.OffloadingStream;
import io.leoplatform.sdk.aws.kinesis.KinesisProducerWriter;
import io.leoplatform.sdk.aws.kinesis.KinesisQueue;
import io.leoplatform.sdk.aws.kinesis.KinesisResults;
import io.leoplatform.sdk.aws.payload.*;
import io.leoplatform.sdk.aws.s3.S3Queue;
import io.leoplatform.sdk.aws.s3.S3Results;
import io.leoplatform.sdk.aws.s3.S3TransferManager;
import io.leoplatform.sdk.aws.s3.S3Writer;
import io.leoplatform.sdk.bus.LoadingBot;
import io.leoplatform.sdk.config.ConnectorConfig;
import io.leoplatform.sdk.payload.ThresholdMonitor;

import javax.inject.Named;
import javax.inject.Singleton;

@Module
final class AWSModule {
    @Singleton
    @Provides
    @Named("AwsLoadingStream")
    static LoadingStream provideLoadingStream(@Named("Proxy") AsyncWorkQueue transferProxy, ExecutorManager executorManager) {
        return new AWSLoadingStream(transferProxy, executorManager);
    }

    @Singleton
    @Provides
    @Named("AwsOffloadingStream")
    static OffloadingStream provideOffloadingStream(ExecutorManager executorManager) {
        return new AWSOffloadingStream(executorManager);
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
    static AsyncWorkQueue provideS3Queue(ExecutorManager executorManager, CompressionWriter compression, S3Writer s3Writer) {
        return new S3Queue(executorManager, compression, s3Writer);
    }

    @Singleton
    @Provides
    static ThresholdMonitor provideThresholdMonitor(ConnectorConfig config, ExecutorManager executorManager) {
        return new InternalThresholdMonitor(config, executorManager);
    }

    @Singleton
    @Provides
    static S3Writer provideS3Writer(ConnectorConfig config, S3TransferManager transferManager, ExecutorManager executorManager) {
        return new S3Writer(config, transferManager, executorManager);
    }

    @Singleton
    @Provides
    static S3TransferManager provideS3TransferManager(ConnectorConfig config, ExecutorManager executorManager,
                                                      S3Results resultsProcessor, LoadingBot bot) {
        return new S3TransferManager(config, executorManager, resultsProcessor, bot);
    }

    @Singleton
    @Provides
    static CompressionWriter provideKinesisCompression(S3JsonPayload streamJson) {
        return new JCraftGzipWriter(streamJson);
    }

    @Singleton
    @Provides
    static S3JsonPayload provideStreamJsonPayload(LoadingBot bot) {
        return new JacksonPayload(bot);
    }

    @Singleton
    @Provides
    static KinesisProducerWriter provideKinesisWrite(ConnectorConfig config, ExecutorManager executorManager, KinesisResults resultsProcessor) {
        return new KinesisProducerWriter(config, executorManager, resultsProcessor);
    }

    @Singleton
    @Provides
    static KinesisResults provideKinesisResults(ThresholdMonitor thresholdMonitor) {
        return new KinesisResults(thresholdMonitor);
    }

    @Singleton
    @Provides
    static S3Results provideS3Results(CompressionWriter compression, KinesisProducerWriter kinesis, ThresholdMonitor thresholdMonitor) {
        return new S3Results(compression, kinesis, thresholdMonitor);
    }
}
