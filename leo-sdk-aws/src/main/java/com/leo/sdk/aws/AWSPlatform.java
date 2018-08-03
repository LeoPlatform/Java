package com.leo.sdk.aws;

import com.leo.sdk.PlatformStream;
import com.leo.sdk.SDKModule;
import com.leo.sdk.WriteQueue;
import com.leo.sdk.aws.kinesis.KinesisCompression;
import com.leo.sdk.aws.kinesis.KinesisResults;
import com.leo.sdk.aws.kinesis.KinesisWrite;
import com.leo.sdk.payload.StreamJsonPayload;
import dagger.Component;

import java.util.List;

@Component(modules = {AWSModule.class, SDKModule.class})
public interface AWSPlatform {
    PlatformStream platformStream();

    AsyncUpload awsUpload();

//    ConnectorConfig connectorConfig();

    List<WriteQueue> writeQueueList();

    KinesisCompression kinesisCompression();

    StreamJsonPayload streamJsonPayload();

    KinesisWrite kinesisWrite();

    KinesisResults kinesisResults();
}
