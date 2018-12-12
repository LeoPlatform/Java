package io.leoplatform.sdk.bus;

public final class Bots {

    public static LoadingBot ofLoading(String name, String destination) {
        return new SimpleLoadingBot(name, new SimpleQueue(destination));
    }

    public static OffloadingBot ofOffloading(String name, String source) {
        return new SimpleOffloadingBot(name, new SimpleQueue(source));
    }

    public static LoadingBot ofChanges() {
        return new ChangeLoadingBot();
    }
}
