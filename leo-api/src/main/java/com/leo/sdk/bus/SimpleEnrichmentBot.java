package com.leo.sdk.bus;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class SimpleEnrichmentBot implements EnrichmentBot {

    private final String name;
    private final List<String> tags;
    private final String description;
    private final StreamQueue source;
    private final StreamQueue destination;

    public SimpleEnrichmentBot(String botName, String source, String destination) {
        this(() -> botName, new SimpleQueue(source), new SimpleQueue(destination));
    }

    public SimpleEnrichmentBot(Bot bot, StreamQueue source, StreamQueue destination) {
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
        this.source = Optional.ofNullable(source)
                .map(s -> new SimpleQueue(s.name(), s.tags()))
                .orElseThrow(() -> new IllegalArgumentException("Missing or invalid source queue"));
        this.destination = Optional.ofNullable(destination)
                .map(d -> new SimpleQueue(d.name(), d.tags()))
                .orElseThrow(() -> new IllegalArgumentException("Missing or invalid destination queue"));
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
    public StreamQueue source() {
        return source;
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
        SimpleEnrichmentBot that = (SimpleEnrichmentBot) o;
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
