package io.leoplatform.sdk.changes;

import io.leoplatform.schema.Field;

import javax.json.JsonArray;
import java.util.concurrent.BlockingQueue;

public interface DomainResolver {
    JsonArray toResultJson(String sourceName, BlockingQueue<Field> fields);
}
