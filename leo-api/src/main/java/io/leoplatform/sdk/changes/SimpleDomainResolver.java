package io.leoplatform.sdk.changes;

import io.leoplatform.schema.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.json.JsonArray;
import java.util.concurrent.BlockingQueue;

@Singleton
public class SimpleDomainResolver implements DomainResolver {
    private static final Logger log = LoggerFactory.getLogger(SimpleDomainResolver.class);

    @Override
    public JsonArray toResultJson(String sourceName, BlockingQueue<Field> fields) {
        log.warn("Using unimplemented domain resolver");
        throw new IllegalStateException("Unimplemented domain resolver");
        //TODO: implement this for others
    }
}
