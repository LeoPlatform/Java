package com.leo.sdk;

import com.leo.sdk.aws.AWSStream;
import com.leo.sdk.aws.DaggerAWSPlatform;
import com.leo.sdk.bus.LoadingBot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeoAWS {
    private static final Logger log = LoggerFactory.getLogger(LeoAWS.class);

    public static PlatformStream load(LoadingBot bot) {
        AsyncWorkQueue transferProxy = DaggerAWSPlatform.builder()
                .build()
                .transferProxy();
        return new AWSStream(transferProxy, bot);
    }
}
