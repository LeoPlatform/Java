package com.leo.sdk;

import com.leo.sdk.aws.AWSStream;
import com.leo.sdk.aws.DaggerAWSPlatform;
import com.leo.sdk.bus.Bots;
import com.leo.sdk.bus.LoadingBot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LeoAWS {
    private static final Logger log = LoggerFactory.getLogger(LeoAWS.class);

    public static PlatformStream of(LoadingBot bot) {
        AsyncWorkQueue transferProxy = DaggerAWSPlatform.builder()
                .build()
                .transferProxy();
        log.info("Created proxy loading stream to {}", bot);
        return new AWSStream(transferProxy, bot);
    }

    public static PlatformStream ofChanges() {
        return LeoAWS.of(Bots.ofChanges());
    }
}
