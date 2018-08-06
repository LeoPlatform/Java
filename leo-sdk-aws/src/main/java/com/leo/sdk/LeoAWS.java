package com.leo.sdk;

import com.leo.sdk.aws.DaggerAWSPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeoAWS {
    private static final Logger log = LoggerFactory.getLogger(LeoAWS.class);

    public static PlatformStream load(Bot bot) {
        return DaggerAWSPlatform.builder()
                .build()
                .platformStream();
    }
}
