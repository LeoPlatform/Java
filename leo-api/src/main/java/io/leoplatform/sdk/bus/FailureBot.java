package io.leoplatform.sdk.bus;

import java.util.List;

public final class FailureBot implements EnrichmentBot {
    @Override
    public StreamQueue source() {
        throw new IllegalArgumentException("Attempt to use a bot when one was not created");
    }

    @Override
    public StreamQueue destination() {
        throw new IllegalArgumentException("Attempt to use a bot when one was not created");
    }

    @Override
    public String name() {
        throw new IllegalArgumentException("Attempt to use a bot when one was not created");
    }

    @Override
    public List<String> tags() {
        throw new IllegalArgumentException("Attempt to use a bot when one was not created");
    }

    @Override
    public String description() {
        throw new IllegalArgumentException("Attempt to use a bot when one was not created");
    }
}
