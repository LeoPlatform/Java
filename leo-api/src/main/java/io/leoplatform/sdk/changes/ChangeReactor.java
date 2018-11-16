package io.leoplatform.sdk.changes;

import io.leoplatform.schema.ChangeEvent;

import java.util.Queue;

public interface ChangeReactor {
    void loadChanges(Queue<ChangeEvent> changes);

    void end();
}
