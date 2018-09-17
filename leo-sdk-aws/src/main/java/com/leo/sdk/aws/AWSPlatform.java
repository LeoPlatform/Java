package com.leo.sdk.aws;

import com.leo.sdk.ExecutorManager;
import com.leo.sdk.SDKModule;
import com.leo.sdk.SDKPlatform;
import com.leo.sdk.aws.kinesis.KinesisProducerWriter;
import com.leo.sdk.aws.kinesis.KinesisResults;
import com.leo.sdk.aws.payload.ThresholdMonitor;
import com.leo.sdk.aws.s3.S3Results;
import com.leo.sdk.aws.s3.S3TransferManager;
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

    S3TransferManager s3TransferManager();

    KinesisProducerWriter kinesisWrite();

    KinesisResults kinesisResults();

    S3Results s3Results();
}
