package com.leo.sdk;

import com.leo.sdk.aws.DaggerAWSLoadingPlatform;
import com.leo.sdk.bus.Bots;
import com.leo.sdk.bus.LoadingBot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * Convenience class for creating a <code>PlatformStream</code> instance.
 */
public final class LeoAWS {
    private static final Logger log = LoggerFactory.getLogger(LeoAWS.class);

    public static PlatformStream of(LoadingBot bot, Executor executor) {

        SDKPlatform sdkPlatform = DaggerExternalExecutorPlatform.builder()
                .executor(executor)
                .build();

        PlatformStream stream = DaggerAWSLoadingPlatform.builder()
                .executorManager(sdkPlatform.executorManager())
                .loadingBot(bot)
                .build()
                .platformStream();
        log.info("Created proxy loading stream to {} with supplied executor", bot.destination().name());
        return stream;
    }

    public static PlatformStream of(LoadingBot bot) {
        SDKPlatform sdkPlatform = DaggerSDKPlatform.builder()
                .build();

        PlatformStream stream = DaggerAWSLoadingPlatform.builder()
                .executorManager(sdkPlatform.executorManager())
                .loadingBot(bot)
                .build()
                .platformStream();

        log.info("Created proxy loading stream to {} with default executor", bot.destination().name());
        return stream;
    }

    public static PlatformStream ofChanges() {
        return LeoAWS.of(Bots.ofChanges());
    }

    public static PlatformStream ofChanges(Executor executor) {
        return LeoAWS.of(Bots.ofChanges(), executor);
    }
}
