package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.ExecutorManager;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.SDKModule;
import com.leo.sdk.aws.kinesis.KinesisProducerWriter;
import com.leo.sdk.aws.kinesis.KinesisResults;
import com.leo.sdk.aws.payload.CompressionWriter;
import com.leo.sdk.aws.payload.ThresholdMonitor;
import com.leo.sdk.aws.s3.S3Results;
import com.leo.sdk.aws.s3.S3TransferManager;
import com.leo.sdk.bus.LoadingBot;
import com.leo.sdk.payload.StreamJsonPayload;
import dagger.BindsInstance;
import dagger.Component;

import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
@Component(modules = {AWSModule.class, SDKModule.class})
public interface AWSPlatform {
    PlatformStream platformStream();

    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder loadingBot(LoadingBot bot);

        @BindsInstance
        Builder executorManager(ExecutorManager executorManager);

        AWSPlatform build();
    }

    WorkQueues workQueues();

    @Named("Proxy")
    AsyncWorkQueue transferProxy();

    @Named("Stream")
    AsyncWorkQueue kinesisQueue();

    @Named("Storage")
    AsyncWorkQueue s3Queue();

    ThresholdMonitor thresholdMonitor();

    S3TransferManager s3TransferManager();

    CompressionWriter kinesisCompression();

    StreamJsonPayload streamJsonPayload();

    KinesisProducerWriter kinesisWrite();

    KinesisResults kinesisResults();

    S3Results s3Results();
}
