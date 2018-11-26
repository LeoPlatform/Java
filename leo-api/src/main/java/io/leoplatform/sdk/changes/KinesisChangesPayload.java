package io.leoplatform.sdk.changes;

import io.leoplatform.schema.ChangeEvent;

import java.util.concurrent.BlockingQueue;

public class KinesisChangesPayload implements ChangeReactor {
    @Override
    public void loadChanges(BlockingQueue<ChangeEvent> changes) {
        //TODO: Transform this to a Kinesis-compatible payload
    }

    @Override
    public void end() {
    }
}
