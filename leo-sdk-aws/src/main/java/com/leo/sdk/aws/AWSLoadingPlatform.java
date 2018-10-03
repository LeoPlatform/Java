package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.ExecutorManager;
import com.leo.sdk.LoadingStream;
import com.leo.sdk.SDKModule;
import com.leo.sdk.aws.payload.CompressionWriter;
import com.leo.sdk.aws.payload.S3JsonPayload;
import com.leo.sdk.aws.s3.S3Results;
import com.leo.sdk.aws.s3.S3TransferManager;
import com.leo.sdk.aws.s3.S3Writer;
import com.leo.sdk.bus.LoadingBot;
import dagger.BindsInstance;
import dagger.Component;

import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
@Component(modules = {AWSModule.class, SDKModule.class})
public interface AWSLoadingPlatform extends AWSPlatform {
    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder executorManager(ExecutorManager executorManager);

        @BindsInstance
        Builder loadingBot(LoadingBot loadingBot);

        AWSLoadingPlatform build();
    }

    LoadingStream loadingStream();

    WorkQueues workQueues();

    @Named("Proxy")
    AsyncWorkQueue transferProxy();

    @Named("Stream")
    AsyncWorkQueue kinesisQueue();

    @Named("Storage")
    AsyncWorkQueue s3Queue();

    CompressionWriter kinesisCompression();

    S3JsonPayload s3JsonPayload();

    S3TransferManager s3TransferManager();

    S3Writer s3Writer();

    S3Results s3Results();
}
