package io.leoplatform.sdk.changes;

import io.leoplatform.schema.ChangeEvent;

import java.util.concurrent.BlockingQueue;

public interface ChangeReactor {
    void loadChanges(BlockingQueue<ChangeEvent> changes);

    void end();
}
