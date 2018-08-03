package com.leo.sdk;

import com.leo.sdk.aws.DaggerAWSPlatform;
import com.leo.sdk.payload.SimplePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;

public class LeoAWS {
    private static final Logger log = LoggerFactory.getLogger(LeoAWS.class);

    public static PlatformStream load(Bot bot) {

        return DaggerAWSPlatform.builder()
                .build()
                .platformStream();
    }

    private static SimplePayload getPayload(int payloadNum) {
        return () -> Json.createObjectBuilder()
                .add("simple", String.format("payload%d", payloadNum))
                .build();
    }
}
