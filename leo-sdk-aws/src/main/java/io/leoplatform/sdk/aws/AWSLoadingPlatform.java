package io.leoplatform.sdk.aws;

import dagger.BindsInstance;
import dagger.Component;
import io.leoplatform.sdk.AsyncWorkQueue;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.LoadingStream;
import io.leoplatform.sdk.SDKModule;
import io.leoplatform.sdk.aws.payload.CompressionWriter;
import io.leoplatform.sdk.aws.payload.S3JsonPayload;
import io.leoplatform.sdk.aws.s3.S3Results;
import io.leoplatform.sdk.aws.s3.S3TransferManager;
import io.leoplatform.sdk.aws.s3.S3Writer;
import io.leoplatform.sdk.bus.LoadingBot;

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
//
//        @BindsInstance
//        Builder loadingStream(LoadingStream executor);
//
//        @BindsInstance
//        Builder payloadWriter(PayloadWriter executor);

        AWSLoadingPlatform build();
    }

    @Named("AwsLoadingStream")
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
