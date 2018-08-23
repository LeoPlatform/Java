package com.leo.sdk.bus;

public final class ChangeLoadingBot implements LoadingBot {

    private static final String NAME = "SchemaChangeDetection";
    private static final StreamQueue CHANGES_QUEUE = new SimpleQueue("SchemaChanges");

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public StreamQueue destination() {
        return CHANGES_QUEUE;
    }

    @Override
    public String toString() {
        return String.format("ChangeLoadingBot{name='%s', destination='%s'}", NAME, CHANGES_QUEUE);
    }
}
