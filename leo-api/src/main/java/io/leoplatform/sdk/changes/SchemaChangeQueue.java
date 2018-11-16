package io.leoplatform.sdk.changes;

import io.leoplatform.schema.ChangeEvent;

public interface SchemaChangeQueue {
    void add(ChangeEvent changeEvent);

    void end();
}
