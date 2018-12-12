package io.leoplatform.sdk.aws;

import com.amazonaws.auth.AWSCredentialsProvider;

public interface AWSResources {

    AWSCredentialsProvider credentials();

    String region();

    String kinesisStream();

    String cronTable();

    String eventTable();

    String storage();
}
