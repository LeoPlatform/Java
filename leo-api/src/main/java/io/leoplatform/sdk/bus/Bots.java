package io.leoplatform.sdk.bus;

public final class Bots {

    public static LoadingBot ofLoading(String name, String destination) {
        return new SimpleLoadingBot(name, new SimpleQueue(destination));
    }

    public static LoadingBot ofChanges() {
        return new ChangeLoadingBot();
    }
}
