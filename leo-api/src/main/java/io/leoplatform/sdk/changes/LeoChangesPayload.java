package io.leoplatform.sdk.changes;

import io.leoplatform.schema.ChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.concurrent.BlockingQueue;

@Singleton
public final class LeoChangesPayload implements ChangeReactor {
    private static final Logger log = LoggerFactory.getLogger(LeoChangesPayload.class);

    @Override
    public void loadChanges(BlockingQueue<ChangeEvent> changes) {
        log.warn("Using unimplemented change payload");
        throw new IllegalStateException("Unimplemented change reactor");
        //TODO: Transform this to the LEO-standard change listener payload
    }

    @Override
    public void end() {
    }
}
