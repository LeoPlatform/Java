package io.leoplatform.sdk.aws;

import dagger.BindsInstance;
import dagger.Component;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.SDKModule;
import io.leoplatform.sdk.SDKPlatform;
import io.leoplatform.sdk.aws.kinesis.KinesisProducerWriter;
import io.leoplatform.sdk.aws.kinesis.KinesisResults;
import io.leoplatform.sdk.payload.ThresholdMonitor;

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
