package com.leo.sdk;

import com.leo.sdk.aws.DaggerAWSPlatform;
import com.leo.sdk.bus.Bots;
import com.leo.sdk.bus.LoadingBot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class for creating a <code>PlatformStream</code> instance.
 */
public final class LeoAWS {
    private static final Logger log = LoggerFactory.getLogger(LeoAWS.class);

    public static PlatformStream of(LoadingBot bot) {
        PlatformStream stream = DaggerAWSPlatform.builder()
                .loadingBot(bot)
                .build()
                .platformStream();
        log.info("Created proxy loading stream to {}", bot);
        return stream;
    }

    public static PlatformStream ofChanges() {
        return LeoAWS.of(Bots.ofChanges());
    }
}
