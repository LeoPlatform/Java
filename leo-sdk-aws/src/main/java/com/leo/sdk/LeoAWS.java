package com.leo.sdk;

import com.leo.sdk.aws.DaggerAWSLoadingPlatform;
import com.leo.sdk.aws.DaggerAWSOffloadingPlatform;
import com.leo.sdk.bus.Bots;
import com.leo.sdk.bus.LoadingBot;
import com.leo.sdk.bus.OffloadingBot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * Convenience class for creating a <code>PlatformStream</code> instance.
 */
public final class LeoAWS {
    private static final Logger log = LoggerFactory.getLogger(LeoAWS.class);

    public static LoadingStream of(LoadingBot bot, Executor executor) {

        SDKPlatform sdkPlatform = DaggerExternalExecutorPlatform.builder()
                .executor(executor)
                .build();

        LoadingStream stream = getLoadingStream(bot, sdkPlatform);
        log.info("Created loading stream to {} queue with supplied executor", bot.destination().name());
        return stream;
    }

    public static LoadingStream of(LoadingBot bot) {
        SDKPlatform sdkPlatform = DaggerSDKPlatform.builder()
                .build();

        LoadingStream stream = getLoadingStream(bot, sdkPlatform);
        log.info("Created loading stream to {} queue with default executor", bot.destination().name());
        return stream;
    }

    public static OffloadingStream of(OffloadingBot bot) {
        SDKPlatform sdkPlatform = DaggerSDKPlatform.builder()
                .build();

        OffloadingStream stream = getOffloadingStream(bot, sdkPlatform);
        log.info("Created offloading stream from {} queue with default executor", bot.source().name());
        return stream;
    }

    public static OffloadingStream of(OffloadingBot bot, Executor executor) {
        SDKPlatform sdkPlatform = DaggerExternalExecutorPlatform.builder()
                .executor(executor)
                .build();

        OffloadingStream stream = getOffloadingStream(bot, sdkPlatform);
        log.info("Created offloading stream from {} queue with default executor", bot.source().name());
        return stream;
    }

    private static LoadingStream getLoadingStream(LoadingBot bot, SDKPlatform sdkPlatform) {
        return DaggerAWSLoadingPlatform.builder()
                .executorManager(sdkPlatform.executorManager())
                .loadingBot(bot)
                .build()
                .loadingStream();
    }

    private static OffloadingStream getOffloadingStream(OffloadingBot bot, SDKPlatform sdkPlatform) {
        return DaggerAWSOffloadingPlatform.builder()
                .executorManager(sdkPlatform.executorManager())
                .offloadingBot(bot)
                .build()
                .offloadingStream();
    }

    public static LoadingStream ofChanges() {
        return LeoAWS.of(Bots.ofChanges());
    }

    public static LoadingStream ofChanges(Executor executor) {
        return LeoAWS.of(Bots.ofChanges(), executor);
    }
}
