package com.leo.sdk.bus;

public class ChangeLoadingBot implements LoadingBot {

    private static final String NAME = "SchemaChangeDetection";
    private static final StreamQueue CHANGES_QUEUE = () -> "SchemaChanges";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public StreamQueue destination() {
        return CHANGES_QUEUE;
    }
}
