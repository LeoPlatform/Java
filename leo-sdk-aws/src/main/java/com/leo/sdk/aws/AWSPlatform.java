package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.SDKModule;
import com.leo.sdk.aws.kinesis.KinesisCompression;
import com.leo.sdk.aws.kinesis.KinesisProducerWriter;
import com.leo.sdk.aws.kinesis.KinesisQueue;
import com.leo.sdk.aws.kinesis.KinesisResults;
import com.leo.sdk.payload.StreamJsonPayload;
import dagger.Component;

import java.util.List;

@Component(modules = {AWSModule.class, SDKModule.class})
public interface AWSPlatform {
    PlatformStream platformStream();

    TransferProxy transferProxy();

    KinesisQueue kinesisQueue();

    List<AsyncWorkQueue> asyncWorkQueues();

    KinesisCompression kinesisCompression();

    StreamJsonPayload streamJsonPayload();

    KinesisProducerWriter kinesisWrite();

    KinesisResults kinesisResults();
}
