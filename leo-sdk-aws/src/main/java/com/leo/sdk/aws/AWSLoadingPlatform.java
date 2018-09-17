package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.ExecutorManager;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.SDKModule;
import com.leo.sdk.aws.payload.CompressionWriter;
import com.leo.sdk.bus.LoadingBot;
import com.leo.sdk.payload.StreamJsonPayload;
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

    PlatformStream platformStream();

    WorkQueues workQueues();

    @Named("Proxy")
    AsyncWorkQueue transferProxy();

    @Named("Stream")
    AsyncWorkQueue kinesisQueue();

    @Named("Storage")
    AsyncWorkQueue s3Queue();

    CompressionWriter kinesisCompression();

    StreamJsonPayload streamJsonPayload();
}
