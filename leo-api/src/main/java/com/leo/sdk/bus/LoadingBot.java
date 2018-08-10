package com.leo.sdk.bus;

import java.util.List;

public class LoadingBot implements Bot {
    private final Bot bot;
    private final StreamQueue destination;

    public LoadingBot(String botName, String queueName) {
        this(() -> botName, () -> queueName);
    }

    public LoadingBot(String name, StreamQueue destination) {
        this(() -> name, destination);
    }

    public LoadingBot(Bot bot, StreamQueue destination) {
        this.bot = bot;
        this.destination = destination;
    }

    @Override
    public String name() {
        return bot.name();
    }

    @Override
    public List<String> tags() {
        return bot.tags();
    }

    @Override
    public String description() {
        return bot.description();
    }

    public StreamQueue destination() {
        return destination;
    }
}
