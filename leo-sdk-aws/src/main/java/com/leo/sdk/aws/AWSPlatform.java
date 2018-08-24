package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.SDKModule;
import com.leo.sdk.aws.kinesis.KinesisCompression;
import com.leo.sdk.aws.kinesis.KinesisProducerWriter;
import com.leo.sdk.aws.kinesis.KinesisResults;
import com.leo.sdk.aws.payload.ThresholdMonitor;
import com.leo.sdk.bus.LoadingBot;
import com.leo.sdk.payload.StreamJsonPayload;
import dagger.BindsInstance;
import dagger.Component;

import javax.inject.Named;
import java.util.List;

@Component(modules = {AWSModule.class, SDKModule.class})
public interface AWSPlatform {
    PlatformStream platformStream();

    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder loadingBot(LoadingBot bot);

        AWSPlatform build();
    }

    WorkQueues workQueues();

    @Named("Proxy")
    AsyncWorkQueue transferProxy();

    ThresholdMonitor thresholdMonitor();

    @Named("Stream")
    AsyncWorkQueue kinesisQueue();

    @Named("Storage")
    AsyncWorkQueue s3Queue();

    List<AsyncWorkQueue> asyncWorkQueues();

    KinesisCompression kinesisCompression();

    StreamJsonPayload streamJsonPayload();

    KinesisProducerWriter kinesisWrite();

    KinesisResults kinesisResults();
}
