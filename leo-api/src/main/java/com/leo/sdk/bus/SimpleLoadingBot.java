package com.leo.sdk.bus;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SimpleLoadingBot implements LoadingBot {
    private final String name;
    private final List<String> tags;
    private final String description;
    private final StreamQueue destination;

    public SimpleLoadingBot(String botName, String queueName) {
        this(() -> botName, new SimpleQueue(queueName));
    }

    public SimpleLoadingBot(String botName, StreamQueue destination) {
        this(() -> botName, destination);
    }

    public SimpleLoadingBot(Bot bot, StreamQueue destination) {
        Bot b = Optional.ofNullable(bot)
                .orElseThrow(() -> new IllegalArgumentException("Missing or invalid bot"));
        this.name = Optional.of(b)
                .map(Bot::name)
                .map(String::trim)
                .filter(n -> !n.isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("Missing bot name"));
        this.tags = Optional.of(b)
                .map(Bot::tags)
                .orElse(Collections.emptyList());
        this.description = Optional.of(b)
                .map(Bot::description)
                .orElse("");
        this.destination = Optional.ofNullable(destination)
                .map(d -> new SimpleQueue(d.name(), d.tags()))
                .orElseThrow(() -> new IllegalArgumentException("Missing or invalid queue"));
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<String> tags() {
        return tags;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public StreamQueue destination() {
        return destination;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SimpleLoadingBot that = (SimpleLoadingBot) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(destination, that.destination);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, destination);
    }

    @Override
    public String toString() {
        String desc = "SimpleLoadingBot{name='%s', tags='%s', description='%s', destination=%s}";
        return String.format(desc, name, String.join(",", tags), description, destination);
    }
}
