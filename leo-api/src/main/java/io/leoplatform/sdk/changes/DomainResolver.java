package io.leoplatform.sdk.changes;

import io.leoplatform.schema.Field;

import javax.json.JsonArray;
import java.util.Queue;

public interface DomainResolver {
    JsonArray toResultJson(String sourceName, Queue<Field> fields);
}
