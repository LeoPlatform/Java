package com.leo.sdk.aws;

import com.leo.sdk.ExecutorManager;
import com.leo.sdk.SDKModule;
import com.leo.sdk.SDKPlatform;
import com.leo.sdk.aws.kinesis.KinesisProducerWriter;
import com.leo.sdk.aws.kinesis.KinesisResults;
import com.leo.sdk.payload.ThresholdMonitor;
import dagger.BindsInstance;
import dagger.Component;

import javax.inject.Singleton;

@Singleton
@Component(modules = {AWSModule.class, SDKModule.class})
public interface AWSPlatform extends SDKPlatform {
    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder executorManager(ExecutorManager executorManager);

        AWSPlatform build();
    }

    ThresholdMonitor thresholdMonitor();

    KinesisProducerWriter kinesisWrite();

    KinesisResults kinesisResults();
}
