package io.leoplatform.sdk.changes;

import io.leoplatform.schema.ChangeEvent;

import java.util.concurrent.BlockingQueue;

public class LeoChangesPayload implements ChangeReactor {
    @Override
    public void loadChanges(BlockingQueue<ChangeEvent> changes) {
        //TODO: Transform this to the LEO-standard change listener payload
    }

    @Override
    public void end() {
    }
}
